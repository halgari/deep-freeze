Deep Freeze
-----------

Deep Freeze is a pure Clojure serialzier that stresses performance and compactness. The aim of this 
project is to become the defacto standard for binary serialization of Clojure data. The interface for the 
library is extremely easy to use. 

Usage
-----------

Simply :require deep-freeze.core and then use the 4 core functions to serialize/deserialize data

(freeze-to-stream item stream)
-----------
serializes the item into the java OutputStream

(freeze-to-array item)
-----------
serializes the item into a byte array

(thaw stream)
-----------
deserializes a clojure structure form the java InputStream

(thaw-from-array array)
-----------
same as thaw but uses a byte array as imput

