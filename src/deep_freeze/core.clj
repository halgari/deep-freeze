(ns deep-freeze.core
	[:import java.io.DataInputStream 
		 java.io.DataOutputStream])

(def ^Integer DOUBLE 1)
(def ^Integer INTEGER 2)
(def ^Integer LONG 3)
(def ^Integer BOOLEAN 4)
(def ^Integer FLOAT 5)
(def ^Integer SHORT 6)
(def ^Integer VECTOR 7)
(def ^Integer MAP 8)
(def ^Integer KEYWORD 9)
(def ^Integer META 10)
(def ^Integer STRING 11)
(def ^Integer BIGINTEGER 12)
(def ^Integer RATIO 13)
(def ^Integer BIGDECIMAL 14)
(def ^Integer NIL 15)
(def ^Integer ATOM 16)
(def ^Integer REF 17)
(def ^Integer AGENT 17)
(def ^Integer KEYWORDINTERN 18)

(set! *warn-on-reflection* true)
		
(def freeze-to-stream nil)

(defn encode-id [id data]
	(bit-or (bit-shift-left data 6) id))

(defn decode-id [data]
	[(bit-and data 63)
	 (bit-shift-right data 6)])
	

(defn write-id
	([^DataOutputStream stream type]
		(.writeInt stream type))
	([^DataOutputStream stream type data]
		(.writeInt stream (encode-id type data))))

(defn new-cache [idx cache]
	{:idx idx :cache cache})

(defn add-cache-value [cache value]
	(:idx (swap! cache
			     #(let [newidx (inc (:idx %))]
					   (new-cache newidx (assoc (:cache %) value newidx))))))

(defn add-decode-value [cache value]
	(swap! cache
		   #(conj @cache value)))


(defn cache-string [cache string]
	(let [cachedvalue (get-in @cache [:cache string])]
		 (if (nil? cachedvalue)
		 	 (add-cache-value cache string)
		 	 cachedvalue)))

(defn write-bigint [i ^DataOutputStream stream]
	(let [ba (.toByteArray i)
	      c (count ba)]
		(.writeShort stream c)
		(.write stream ba 0 c)))

(defn read-bigint [^DataInputStream stream]
	(let [size (.readShort stream)
	      ba (byte-array size)]
	      (.read stream ba 0 size)
	      (java.math.BigInteger. ba)))
	      
(defprotocol Freezeable 
	(*freeze [obj ^DataOutputStream stream cache]))		

(extend-protocol Freezeable
	java.lang.Double
		(*freeze [itm ^DataOutputStream stream cache]
			(write-id stream DOUBLE)
			(.writeDouble stream itm))
	Integer
		(*freeze [itm ^DataOutputStream stream cache]
			(write-id stream INTEGER)
			(.writeInt stream itm))
	java.lang.Long
		(*freeze [itm ^DataOutputStream stream cache]
			(write-id stream LONG)
			(.writeLong stream itm))
	java.lang.Boolean
		(*freeze [itm ^DataOutputStream stream cache]
			(write-id stream BOOLEAN)
			(.writeBoolean stream itm))
	java.lang.Float
		(*freeze [itm ^DataOutputStream stream cache]
			(write-id stream FLOAT)
			(.writeFloat stream itm))
	java.lang.Short
		(*freeze [itm ^DataOutputStream stream cache]
			(write-id stream SHORT)
			(.writeLong stream itm))
	clojure.lang.PersistentVector
		(*freeze [itm ^DataOutputStream stream cache]
			(write-id stream VECTOR (count itm))
			(doseq [i itm]
				(freeze-to-stream i stream cache)))
	clojure.lang.PersistentArrayMap
		(*freeze [itm ^DataOutputStream stream cache]
			(write-id stream MAP (count itm))
			(doseq [i itm]
				(freeze-to-stream (first i) stream cache)
				(freeze-to-stream (second i) stream cache)))
	clojure.lang.PersistentStructMap
		(*freeze [itm ^DataOutputStream stream cache]
			(write-id stream MAP (count itm))
			(doseq [i itm]
				(freeze-to-stream (first i) stream cache)
				(freeze-to-stream (second i) stream cache)))	
	clojure.lang.PersistentHashMap
		(*freeze [itm ^DataOutputStream stream cache]
			(write-id stream MAP (count itm))
			(doseq [i itm]
				(freeze-to-stream (first i) stream cache)
				(freeze-to-stream (second i) stream cache)))			
	clojure.lang.Keyword
		(*freeze [itm ^DataOutputStream stream cache]
			(let [idx (cache-string cache (name itm))]
				(if (nil? idx)
					(do 
						(write-id stream KEYWORD)
						(.writeUTF stream (name itm)))
					(write-id stream KEYWORDINTERN idx))))
	java.lang.String
		(*freeze [itm ^DataOutputStream stream cache]
			(write-id stream STRING)
			(.writeUTF stream itm))
	java.math.BigInteger
		(*freeze [itm ^DataOutputStream stream cache]
			(write-id stream BIGINTEGER)
			(write-bigint itm stream))
	java.math.BigDecimal
		(*freeze [itm ^DataOutputStream stream cache]
			(write-id stream BIGDECIMAL)
			(write-bigint (.unscaledValue itm) stream)
			(.writeInt stream (.scale itm)))
	clojure.lang.Ratio
		(*freeze [itm ^DataOutputStream stream cache]
			(write-id stream RATIO)
			(write-bigint (.numerator itm) stream)
			(write-bigint (.denominator itm) stream))
	clojure.lang.LazySeq
		(*freeze [itm ^DataOutputStream stream cache]
			(write-id stream VECTOR (count itm))
			(doseq [i itm]
				(freeze-to-stream i stream cache)))
	clojure.lang.Atom
		(*freeze [itm ^DataOutputStream stream cache]
			(write-id stream ATOM)
			(freeze-to-stream @itm stream cache))
	clojure.lang.Ref
		(*freeze [itm ^DataOutputStream stream cache]
			(write-id stream REF)
			(freeze-to-stream @itm stream cache))
	clojure.lang.Agent
		(*freeze [itm ^DataOutputStream stream cache]
			(write-id stream AGENT)
			(freeze-to-stream @itm stream cache))
	nil
		(*freeze [itm ^DataOutputStream stream cache]
			(write-id stream NIL))
		
	Object
		(*freeze [itm ^DataOutputStream stream cache]
			(throw (java.lang.Exception. (str "Can't freeze " (class itm))))))


(defn freeze-to-stream 
	([item ^DataOutputStream stream]
		(freeze-to-stream item stream (atom (new-cache 0 {}))))
	([item ^DataOutputStream stream cache]
		(if (not (nil? (meta item)))
			(do (write-id stream META)
				(freeze-to-stream (meta item) stream cache)))
		(*freeze item stream cache)))

(defn freeze-to-array [item]
	(let [ba (java.io.ByteArrayOutputStream.)
		  stream (DataOutputStream. ba)]
		(freeze-to-stream item stream)
		(.flush stream)
		(.toByteArray ba)))

(def thaw-from-stream nil)

(defmulti thaw (fn [type data cache ^DataInputStream stream] type))
(defmethod thaw DOUBLE
	[type data cache ^DataInputStream stream]
	(.readDouble stream))
(defmethod thaw INTEGER
	[type data cache ^DataInputStream stream]
	(.readInt stream))
(defmethod thaw LONG
	[type data cache ^DataInputStream stream]
	(.readLong stream))
(defmethod thaw BOOLEAN
	[type data cache ^DataInputStream stream]
	(.readBoolean stream))
(defmethod thaw SHORT
	[type data cache ^DataInputStream stream]
	(.readShort stream))
(defmethod thaw FLOAT
	[type data cache ^DataInputStream stream]
	(.readFloat stream))
(defmethod thaw VECTOR
	[type data cache ^DataInputStream stream]
	(vec (map (fn [x] (thaw-from-stream stream)) 
				          (range data))))
(defmethod thaw MAP
	[type data cache ^DataInputStream stream]
	(let [trans (transient {})]
		(doseq [x (range data)] (assoc! trans 
									   (thaw-from-stream stream) 
									   (thaw-from-stream stream)))
		(persistent! trans)))
(defmethod thaw META
	[type data cache ^DataInputStream stream]
	(let [m (thaw stream)]
		(with-meta (thaw stream) m)))
(defmethod thaw KEYWORD
	[type data cache ^DataInputStream stream]
	(let [s (.readUTF stream)]
		(add-cache-value cache s)
		(keyword s)))
(defmethod thaw KEYWORDINTERN
	[type data cache ^DataInputStream stream]
	(keyword (get cache data)))
(defmethod thaw STRING
	[type data cache ^DataInputStream stream]
	(.readUTF stream))
(defmethod thaw BIGINTEGER
	[type data cache ^DataInputStream stream]
	(read-bigint stream))
(defmethod thaw ATOM
	[type data cache ^DataInputStream stream]
	(atom (thaw stream)))
(defmethod thaw REF
	[type data cache ^DataInputStream stream]
	(ref (thaw stream)))
(defmethod thaw AGENT
	[type data cache ^DataInputStream stream]
	(agent (thaw stream)))
(defmethod thaw BIGDECIMAL
	[type data cache ^DataInputStream stream]
	(java.math.BigDecimal. (read-bigint stream) (.readInt stream)))
(defmethod thaw RATIO
	[type data cache ^DataInputStream stream]
	(/ (read-bigint stream) (read-bigint stream)))
(defmethod thaw NIL
	[type data cache ^DataInputStream stream]
	nil)

(defn thaw-from-stream 
	([^DataInputStream stream]
		(thaw-from-stream (atom (new-cache 0 {})) stream))
	([cache ^DataInputStream stream]
		(let [[id data] (decode-id (.readInt stream))]
			(thaw id data cache stream))))

(defn thaw-from-array [array]
	(thaw-from-stream (DataInputStream. (java.io.ByteArrayInputStream. array))))

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

(def dat2 (vector (for [x (range 1000)] {:thiskeywordtest 1 :ykeywordtest (str "foo" x) :zat x})))

(defn bench [] (time (doseq [x (range 100)] (clone dat))))

(clone dat) 
