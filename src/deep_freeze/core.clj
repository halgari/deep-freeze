(ns deep-freeze.core
  [:import java.io.DataInputStream java.io.DataOutputStream])

;; TODO
;; * Possible to de/serialize types, records, functions?
;; * Investigate performance effect of memoizing serialization when possible
;;   (esp. keywords).
;; * Investigate performance and compression effect of interning keywords.
;; * Support Snappy stream compression
;; * Add streaming unit tests

;; Integer ids used instead of Bytes to provide room for type metadata
(def ^:const ^Integer INTEGER   0)
(def ^:const ^Integer LONG      1)
(def ^:const ^Integer FLOAT     2)
(def ^:const ^Integer DOUBLE    3)
(def ^:const ^Integer BIGINT    4)
(def ^:const ^Integer BIGDEC    5)
(def ^:const ^Integer RATIO     6)
(def ^:const ^Integer BOOLEAN   7)
(def ^:const ^Integer CHAR      8)
(def ^:const ^Integer STRING    9)
(def ^:const ^Integer KEYWORD   10)
(def ^:const ^Integer LIST      11)
(def ^:const ^Integer VECTOR    12)
(def ^:const ^Integer SET       13)
(def ^:const ^Integer MAP       14)
(def ^:const ^Integer SEQ       15)
(def ^:const ^Integer ATOM      16)
(def ^:const ^Integer REF       17)
(def ^:const ^Integer AGENT     18)
(def ^:const ^Integer META      19)
(def ^:const ^Integer NIL       20)

;; TODO Still to implement support for these
;;(def ^:const ^Integer TYPE      21) ; clojure.lang.IType
;;(def ^:const ^Integer RECORD    22) ; clojure.lang.IRecord
;;(def ^:const ^Integer FN        23) ; clojure.lang.IFn

(declare freeze-to-stream!)
(declare thaw-from-stream!)

(defn encode-id [id data] (bit-or (bit-shift-left data 6) id))
(defn decode-id [data]    [(bit-and data 63) (bit-shift-right data 6)])

(defn write-id!
  ([^DataOutputStream stream type]      (.writeInt stream type))
  ([^DataOutputStream stream type data] (.writeInt stream (encode-id type data))))

(defn write-bigint! [i ^DataOutputStream stream]
  (let [ba (.toByteArray i)
        c  (count ba)]
    (.writeShort stream c)
    (.write stream ba 0 c)))

(defn read-bigint! [^DataInputStream stream]
  (let [size (.readShort stream)
        ba   (byte-array size)]
    (.read stream ba 0 size)
    (java.math.BigInteger. ba)))

(defprotocol Freezeable (*freeze [obj ^DataOutputStream stream]))

(extend-protocol Freezeable
  java.lang.Integer
  (*freeze [itm ^DataOutputStream stream]
    (write-id! stream INTEGER)
    (.writeInt stream itm))
  java.lang.Long
  (*freeze [itm ^DataOutputStream stream]
    (write-id! stream LONG)
    (.writeLong stream itm))
  java.lang.Float
  (*freeze [itm ^DataOutputStream stream]
    (write-id! stream FLOAT)
    (.writeFloat stream itm))
  java.lang.Double
  (*freeze [itm ^DataOutputStream stream]
    (write-id! stream DOUBLE)
    (.writeDouble stream itm))
  clojure.lang.BigInt
  (*freeze [itm ^DataOutputStream stream]
    (write-id! stream BIGINT)
    (write-bigint! (.toBigInteger itm) stream))
  java.math.BigDecimal
  (*freeze [itm ^DataOutputStream stream]
    (write-id! stream BIGDEC)
    (write-bigint! (.unscaledValue itm) stream)
    (.writeInt stream (.scale itm)))
  clojure.lang.Ratio
  (*freeze [itm ^DataOutputStream stream]
    (write-id! stream RATIO)
    (write-bigint! (.numerator itm) stream)
    (write-bigint! (.denominator itm) stream))
  java.lang.Boolean
  (*freeze [itm ^DataOutputStream stream]
    (write-id! stream BOOLEAN)
    (.writeBoolean stream itm))
  java.lang.Character
  (*freeze [itm ^DataOutputStream stream]
    (write-id! stream CHAR)
    (.writeChar stream (int itm)))
  java.lang.String
  (*freeze [itm ^DataOutputStream stream]
    (write-id! stream STRING)
    (.writeUTF stream itm))
  clojure.lang.Keyword
  (*freeze [itm ^DataOutputStream stream]
    (write-id! stream KEYWORD)
    (.writeUTF stream (name itm)))
  clojure.lang.IPersistentList
  (*freeze [itm ^DataOutputStream stream]
    (write-id! stream LIST (count itm))
    (doseq [i itm] (freeze-to-stream! i stream)))
  clojure.lang.IPersistentVector
  (*freeze [itm ^DataOutputStream stream]
    (write-id! stream VECTOR (count itm))
    (doseq [i itm] (freeze-to-stream! i stream)))
  clojure.lang.IPersistentSet
  (*freeze [itm ^DataOutputStream stream]
    (write-id! stream SET (count itm))
    (doseq [i itm] (freeze-to-stream! i stream)))
  clojure.lang.IPersistentMap
  (*freeze [itm ^DataOutputStream stream]
    (write-id! stream MAP (count itm))
    (doseq [i itm]
      (freeze-to-stream! (first i) stream)
      (freeze-to-stream! (second i) stream)))
  clojure.lang.ISeq ; Anything else implementing ISeq
  (*freeze [itm ^DataOutputStream stream]
    (write-id! stream SEQ (count itm))
    (doseq [i itm] (freeze-to-stream! i stream)))
  clojure.lang.Atom
  (*freeze [itm ^DataOutputStream stream]
    (write-id! stream ATOM)
    (freeze-to-stream! @itm stream))
  clojure.lang.Ref
  (*freeze [itm ^DataOutputStream stream]
    (write-id! stream REF)
    (freeze-to-stream! @itm stream))
  clojure.lang.Agent
  (*freeze [itm ^DataOutputStream stream]
    (write-id! stream AGENT)
    (freeze-to-stream! @itm stream))
  nil
  (*freeze [itm ^DataOutputStream stream]
    (write-id! stream NIL))
  Object
  (*freeze [itm ^DataOutputStream stream]
    (throw (java.lang.Exception.
            (str "Don't know how to freeze " (class itm) ". "
                 "Consider extending Freezeable?")))))

(defn freeze-to-stream! [item ^DataOutputStream stream]
  (if-let [m (meta item)]
    (do (write-id! stream META)
        (freeze-to-stream! m stream)))
  (*freeze item stream))

(defn freeze-to-array
  ([item] (freeze-to-array item true))
  ([item compress?]
     (let [ba (java.io.ByteArrayOutputStream.)
           stream (DataOutputStream. ba)]
       (freeze-to-stream! item stream)
       (.flush stream)
       (if compress?
         (org.xerial.snappy.Snappy/compress (.toByteArray ba))
         (.toByteArray ba)))))

(defmulti thaw (fn [type data ^DataInputStream stream] type))
(defmethod thaw INTEGER
  [type data ^DataInputStream stream]
  (.readInt stream))
(defmethod thaw LONG
  [type data ^DataInputStream stream]
  (.readLong stream))
(defmethod thaw FLOAT
  [type data ^DataInputStream stream]
  (.readFloat stream))
(defmethod thaw DOUBLE
  [type type ^DataInputStream stream]
  (.readDouble stream))
(defmethod thaw BIGINT
  [type data ^DataInputStream stream]
  (bigint (read-bigint! stream)))
(defmethod thaw BIGDEC
  [type data ^DataInputStream stream]
  (java.math.BigDecimal. (read-bigint! stream) (.readInt stream)))
(defmethod thaw RATIO
  [type data ^DataInputStream stream]
  (/ (read-bigint! stream) (read-bigint! stream)))
(defmethod thaw BOOLEAN
  [type data ^DataInputStream stream]
  (.readBoolean stream))
(defmethod thaw CHAR
  [type data ^DataInputStream stream]
  (.readChar stream))
(defmethod thaw STRING
  [type data ^DataInputStream stream]
  (.readUTF stream))
(defmethod thaw KEYWORD
  [type data ^DataInputStream stream]
  (keyword (.readUTF stream)))
(defmethod thaw LIST
  [type data ^DataInputStream stream]
  (apply list (repeatedly data (fn [] (thaw-from-stream! stream)))))
(defmethod thaw VECTOR
  [type data ^DataInputStream stream]
  (vec (repeatedly data (fn [] (thaw-from-stream! stream)))))
(defmethod thaw SET
  [type data ^DataInputStream stream]
  (set (repeatedly data (fn [] (thaw-from-stream! stream)))))
(defmethod thaw MAP
  [type data ^DataInputStream stream]
  (persistent!
   (reduce (fn [m _]
             (assoc! m (thaw-from-stream! stream) (thaw-from-stream! stream)))
           (transient {}) (range data))))
(defmethod thaw SEQ
  [type data ^DataInputStream stream]
  (repeatedly data (fn [] (thaw-from-stream! stream))))
(defmethod thaw ATOM
  [type data ^DataInputStream stream]
  (atom (thaw-from-stream! stream)))
(defmethod thaw REF
  [type data ^DataInputStream stream]
  (ref (thaw-from-stream! stream)))
(defmethod thaw AGENT
  [type data ^DataInputStream stream]
  (agent (thaw-from-stream! stream)))
(defmethod thaw META
  [type data ^DataInputStream stream]
  (let [m (thaw-from-stream! stream)]
    (with-meta (thaw-from-stream! stream) m)))
(defmethod thaw NIL
  [type data ^DataInputStream stream]
  nil)

(defn thaw-from-stream!
  [^DataInputStream stream]
  (let [[type data] (decode-id (.readInt stream))]
    (thaw type data stream)))

(defn thaw-from-array
  ([array] (thaw-from-array array true))
  ([array compressed?]
     (thaw-from-stream!
      (DataInputStream.
       (java.io.ByteArrayInputStream.
        (if compressed?
          (org.xerial.snappy.Snappy/uncompress array)
          array))))))

;;; Benchmarking
(comment
  (defn- array-roundtrip [item compress?]
    (thaw-from-array (freeze-to-array item compress?) compress?))

  (def stressrec
   {:longs   (range 1000)
    :doubles (repeatedly 1000 rand)
    :strings (repeat 1000 "This is a UTF8 string! ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ")})

  (def pre-frozen-compressed   (freeze-to-array stressrec true))
  (def pre-frozen-uncompressed (freeze-to-array stressrec false))

  (time (dotimes [_ 1000] (array-roundtrip stressrec)))

  ;;; Intel Core i7 2.67Ghz notebook, 1Gb memory virtual machine

  ;;; Roundtrips
  ;; 1.0.0: 7300ms
  ;; 1.1.0: 3700ms
  ;; 1.2.0: 4800ms (with Snappy compression)
  ;; clj-serializer 0.1.3: 5200ms (without compression)

  ;;; Deserialization only, 1.2.0
  (time (dotimes [_ 10000] (thaw-from-array pre-frozen-compressed true)))    ; 2100ms
  (time (dotimes [_ 10000] (thaw-from-array pre-frozen-uncompressed false))) ; 140ms

  ;;; Compression
  (count (String. (freeze-to-array stressrec false))) ; 67,496 chars
  (count (String. (freeze-to-array stressrec true)))  ; 17,554 chars
  ;; i.e. a 26% ratio in string format

  )