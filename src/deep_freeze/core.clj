(ns deep-freeze.core)

(def ^java.lang.Integer DOUBLE 1)
(def ^java.lang.Integer INTEGER 2)
(def ^java.lang.Integer LONG 3)
(def ^java.lang.Integer BOOLEAN 4)
(def ^java.lang.Integer FLOAT 5)
(def ^java.lang.Integer SHORT 6)
(def ^java.lang.Integer VECTOR 7)
(def ^java.lang.Integer MAP 8)
(def ^java.lang.Integer KEYWORD 9)
(def ^java.lang.Integer META 10)
(def ^java.lang.Integer STRING 11)
(def ^java.lang.Integer BIGINTEGER 12)
(def ^java.lang.Integer RATIO 13)
(def ^java.lang.Integer BIGDECIMAL 14)
(def ^java.lang.Integer NIL 15)

(set! *warn-on-reflection* true)
		
(def freeze-to-stream nil)


(defn write-bigint [i ^java.io.DataOutputStream stream]
	(let [ba (.toByteArray i)
	      c (count ba)]
		(.writeShort stream c)
		(.write stream ba 0 c)))

(defn read-bigint [^java.io.DataInputStream stream]
	(let [size (.readShort stream)
	      ba (byte-array size)]
	      (.read stream ba 0 size)
	      (java.math.BigInteger. ba)))
	      
(defprotocol Freezeable 
	(*freeze [obj ^java.io.DataOutputStream stream]))		

(extend-protocol Freezeable
	java.lang.Double
		(*freeze [itm ^java.io.DataOutputStream stream]
			(.writeInt stream DOUBLE)
			(.writeDouble stream itm))
	java.lang.Integer
		(*freeze [itm ^java.io.DataOutputStream stream]
			(.writeInt stream INTEGER)
			(.writeInt stream itm))
	java.lang.Long
		(*freeze [itm ^java.io.DataOutputStream stream]
			(.writeInt stream LONG)
			(.writeLong stream itm))
	java.lang.Boolean
		(*freeze [itm ^java.io.DataOutputStream stream]
			(.writeInt stream BOOLEAN)
			(.writeBoolean stream itm))
	java.lang.Float
		(*freeze [itm ^java.io.DataOutputStream stream]
			(.writeInt stream FLOAT)
			(.writeFloat stream itm))
	java.lang.Short
		(*freeze [itm ^java.io.DataOutputStream stream]
			(.writeInt stream SHORT)
			(.writeLong stream itm))
	clojure.lang.PersistentVector
		(*freeze [itm ^java.io.DataOutputStream stream]
			(.writeInt stream VECTOR)
			(.writeInt stream (count itm))
			(doseq [i itm]
				(freeze-to-stream i stream)))
	clojure.lang.PersistentArrayMap
		(*freeze [itm ^java.io.DataOutputStream stream]
			(.writeInt stream MAP)
			(.writeInt stream (count itm))
			(doseq [i itm]
				(freeze-to-stream (first i) stream)
				(freeze-to-stream (second i) stream)))
	clojure.lang.PersistentStructMap
		(*freeze [itm ^java.io.DataOutputStream stream]
			(.writeInt stream MAP)
			(.writeInt stream (count itm))
			(doseq [i itm]
				(freeze-to-stream (first i) stream)
				(freeze-to-stream (second i) stream)))	
	clojure.lang.PersistentHashMap
		(*freeze [itm ^java.io.DataOutputStream stream]
			(.writeInt stream MAP)
			(.writeInt stream (count itm))
			(doseq [i itm]
				(freeze-to-stream (first i) stream)
				(freeze-to-stream (second i) stream)))			
	clojure.lang.Keyword
		(*freeze [itm ^java.io.DataOutputStream stream]
			(.writeInt stream KEYWORD)
			(.writeUTF stream (name itm)))
	java.lang.String
		(*freeze [itm ^java.io.DataOutputStream stream]
			(.writeInt stream STRING)
			(.writeUTF stream itm))
	java.math.BigInteger
		(*freeze [itm ^java.io.DataOutputStream stream]
			(.writeInt stream BIGINTEGER)
			(write-bigint itm stream))
	java.math.BigDecimal
		(*freeze [itm ^java.io.DataOutputStream stream]
			(.writeInt stream BIGDECIMAL)
			(write-bigint (.unscaledValue itm) stream)
			(.writeInt stream (.scale itm)))
	clojure.lang.Ratio
		(*freeze [itm ^java.io.DataOutputStream stream]
			(.writeInt stream RATIO)
			(write-bigint (.numerator itm) stream)
			(write-bigint (.denominator itm) stream))
	clojure.lang.LazySeq
		(*freeze [itm ^java.io.DataOutputStream stream]
			(.writeInt stream VECTOR)
			(.writeInt stream (count itm))
			(doseq [i itm]
				(freeze-to-stream i stream))) 
	nil
		(*freeze [itm ^java.io.DataOutputStream stream]
			(.writeInt stream NIL))
		
	Object
		(*freeze [itm ^java.io.DataOutputStream stream]
			(throw (java.lang.Exception. (str "Can't freeze" (class itm))))))


(defn freeze-to-stream [item ^java.io.DataOutputStream stream]
	(if (not (nil? (meta item)))
		(do (.writeInt stream META)
		    (freeze-to-stream (meta item) stream)))
	(*freeze item stream))

(defn freeze-to-array [item]
	(let [ba (java.io.ByteArrayOutputStream.)
		  stream (java.io.DataOutputStream. ba)]
		(freeze-to-stream item stream)
		(.flush stream)
		(.toByteArray ba)))

(defmulti thaw (fn [^java.io.DataInputStream stream] (.readInt stream)))
(defmethod thaw DOUBLE
	[^java.io.DataInputStream stream]
	(.readDouble stream))
(defmethod thaw INTEGER
	[^java.io.DataInputStream stream]
	(.readInt stream))
(defmethod thaw LONG
	[^java.io.DataInputStream stream]
	(.readLong stream))
(defmethod thaw BOOLEAN
	[^java.io.DataInputStream stream]
	(.readBoolean stream))
(defmethod thaw SHORT
	[^java.io.DataInputStream stream]
	(.readShort stream))
(defmethod thaw FLOAT
	[^java.io.DataInputStream stream]
	(.readFloat stream))
(defmethod thaw VECTOR
	[^java.io.DataInputStream stream]
	(let [cnt (.readInt stream)]
		(vec (map (fn [x] (thaw stream)) (range cnt)))))
(defmethod thaw MAP
	[^java.io.DataInputStream stream]
	(let [cnt (.readInt stream)
	      trans (transient {})]
		(doseq [x (range cnt)] (assoc! trans (thaw stream) (thaw stream)))
		(persistent! trans)))
(defmethod thaw META
	[^java.io.DataInputStream stream]
	(let [m (thaw stream)]
		(with-meta (thaw stream) m)))
(defmethod thaw KEYWORD
	[^java.io.DataInputStream stream]
	(keyword (.readUTF stream)))
(defmethod thaw STRING
	[^java.io.DataInputStream stream]
	(.readUTF stream))
(defmethod thaw BIGINTEGER
	[^java.io.DataInputStream stream]
	(read-bigint stream))
(defmethod thaw BIGDECIMAL
	[^java.io.DataInputStream stream]
	(java.math.BigDecimal. (read-bigint stream) (.readInt stream)))
(defmethod thaw RATIO
	[^java.io.DataInputStream stream]
	(/ (read-bigint stream) (read-bigint stream)))
(defmethod thaw NIL
	[^java.io.DataInputStream stream]
	nil)

(defn thaw-from-array [array]
	(thaw (java.io.DataInputStream. (java.io.ByteArrayInputStream. array))))

(defn clone [data]
	(let [ba (time (freeze-to-array data))]
		(println (str "size: " (count ba)))
		(time (thaw-from-array ba))
		nil))
(defn clone-clojure [data]
	(let [s (time (print-str data))]
		(println (str "size: " (count s)))
		(time (read-string s))
		nil))

(def dat (clojure.xml/parse "C:\\scratchspace\\systems.xml\\dbo.mapSolarSystems.xml"))

(def dat2 (vector (for [x (range 1000)] {:x 1 :y (str "foo" x) :zat x})))

(defn bench [] (time (doseq [x (range 100)] (clone dat2))))
