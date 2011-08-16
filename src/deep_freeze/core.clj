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

		
(def freeze-to-stream nil)

(defprotocol Freezeable 
	(*freeze [obj stream]))

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

(defn thaw-from-array [array]
	(thaw (java.io.DataInputStream. (java.io.ByteArrayInputStream. array))))

(defn clone [data]
	(thaw-from-array (freeze-to-array data)))


