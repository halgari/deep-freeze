(ns deep-freeze.core)

(def DOUBLE 1)
(def INTEGER 2)
(def LONG 3)
(def BOOLEAN 4)
(def FLOAT 5)
(def SHORT 6)
(def VECTOR 7)
(def MAP 8)
(def KEYWORD 9)
(def META 10)
(def STRING 11)
(def BIGINTEGER 12)
(def RATIO 13)
(def BIGDECIMAL 14)

		
(def freeze-to-stream nil)

(defprotocol Freezeable 
	(*freeze [obj stream]))

(defn write-bigint [i stream]
	(let [ba (.toByteArray i)
	      c (count ba)]
		(.writeShort stream c)
		(.write stream ba 0 c)))

(defn read-bigint [stream]
	(let [size (.readShort stream)
	      ba (byte-array size)]
	      (.read stream ba 0 size)
	      (java.math.BigInteger. ba)))
	      
		

(extend-protocol Freezeable
	java.lang.Double
		(*freeze [itm stream]
			(.writeInt stream DOUBLE)
			(.writeDouble stream itm))
	java.lang.Integer
		(*freeze [itm stream]
			(.writeInt stream INTEGER)
			(.writeInt stream itm))
	java.lang.Long
		(*freeze [itm stream]
			(.writeInt stream LONG)
			(.writeLong stream itm))
	java.lang.Boolean
		(*freeze [itm stream]
			(.writeInt stream BOOLEAN)
			(.writeBoolean stream itm))
	java.lang.Float
		(*freeze [itm stream]
			(.writeInt stream FLOAT)
			(.writeFloat stream itm))
	java.lang.Short
		(*freeze [itm stream]
			(.writeInt stream SHORT)
			(.writeLong stream itm))
	clojure.lang.PersistentVector
		(*freeze [itm stream]
			(.writeInt stream VECTOR)
			(.writeInt stream (count itm))
			(doseq [i itm]
				(freeze-to-stream i stream)))
	clojure.lang.PersistentArrayMap
		(*freeze [itm stream]
			(.writeInt stream MAP)
			(.writeInt stream (count itm))
			(doseq [i itm]
				(freeze-to-stream (first i) stream)
				(freeze-to-stream (second i) stream)))
	clojure.lang.Keyword
		(*freeze [itm stream]
			(.writeInt stream KEYWORD)
			(.writeUTF stream (name itm)))
	java.lang.String
		(*freeze [itm stream]
			(.writeInt stream STRING)
			(.writeUTF stream itm))
	java.math.BigInteger
		(*freeze [itm stream]
			(.writeInt stream BIGINTEGER)
			(write-bigint itm stream))
	java.math.BigDecimal
		(*freeze [itm stream]
			(.writeInt stream BIGDECIMAL)
			(write-bigint (.unscaledValue itm) stream)
			(.writeInt stream (.scale itm)))
	clojure.lang.Ratio
		(*freeze [itm stream]
			(.writeInt stream RATIO)
			(write-bigint (.numerator itm) stream)
			(write-bigint (.denominator itm) stream))
		
	Object
		(*freeze [itm stream]
			(throw (java.lang.Exception. (str "Can't freeze" (class itm))))))


(defn freeze-to-stream [item stream]
	(if (isa? (class item) clojure.lang.IObj)
		(if (not (nil? (meta item)))
			(do (.writeInt stream META)
			    (freeze-to-stream (meta item) stream))))
	(*freeze item stream))

(defn freeze-to-array [item]
	(let [ba (java.io.ByteArrayOutputStream.)
		  stream (java.io.DataOutputStream. ba)]
		(freeze-to-stream item stream)
		(.flush stream)
		(.toByteArray ba)))

(defmulti thaw #(.readInt %))
(defmethod thaw DOUBLE
	[stream]
	(.readDouble stream))
(defmethod thaw INTEGER
	[stream]
	(.readInt stream))
(defmethod thaw LONG
	[stream]
	(.readLong stream))
(defmethod thaw BOOLEAN
	[stream]
	(.readBoolean stream))
(defmethod thaw SHORT
	[stream]
	(.readShort stream))
(defmethod thaw FLOAT
	[stream]
	(.readFloat stream))
(defmethod thaw VECTOR
	[stream]
	(let [cnt (.readInt stream)]
		(vec (map (fn [x] (thaw stream)) (range cnt)))))
(defmethod thaw MAP
	[stream]
	(let [cnt (.readInt stream)
	      trans (transient {})]
		(doseq [x (range cnt)] (assoc! trans (thaw stream) (thaw stream)))
		(persistent! trans)))
(defmethod thaw META
	[stream]
	(let [m (thaw stream)]
		(with-meta (thaw stream) m)))
(defmethod thaw KEYWORD
	[stream]
	(keyword (.readUTF stream)))
(defmethod thaw STRING
	[stream]
	(.readUTF stream))
(defmethod thaw BIGINTEGER
	[stream]
	(read-bigint stream))
(defmethod thaw BIGDECIMAL
	[stream]
	(java.math.BigDecimal. (read-bigint stream) (.readInt stream)))
(defmethod thaw RATIO
	[stream]
	(/ (read-bigint stream) (read-bigint stream)))

(defn thaw-from-array [array]
	(thaw (java.io.DataInputStream. (java.io.ByteArrayInputStream. array))))

(defn clone [data]
	(let [ba (freeze-to-array data)]
		(println (str "size: " (count ba)))
		(thaw-from-array ba)))


