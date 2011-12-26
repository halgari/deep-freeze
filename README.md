#Deep-Freeze

Deep Freeze is a pure Clojure serialzier that stresses performance and compactness. The aim of this 
project is to become the defacto standard for binary serialization of Clojure data. The interface for the 
library is extremely easy to use.

Supports Google's [Snappy](http://code.google.com/p/snappy-java/) for high-speed de/compression.

##Usage

Include in a Leiningen project by adding this to your dependencies:

```clojure
[ptaoussanis/deep-freeze "1.2.1"]
```

Simply **:require deep-freeze.core** and then use the 4 core functions to serialize/deserialize data.

###(freeze-to-stream item stream)
Serializes the item into the java OutputStream.

###(freeze-to-array item)
Serializes the item into a byte array.

###(thaw stream)
Deserializes a clojure structure form the java InputStream.

###(thaw-from-array array)
Same as thaw but uses a byte array as imput.
