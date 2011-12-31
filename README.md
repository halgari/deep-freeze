#Deep-Freeze

Deep Freeze is a pure Clojure serializer that stresses performance and compactness. The aim of this 
project is to become the defacto standard for binary serialization of Clojure data. The interface for the 
library is extremely easy to use.

Supports Google's [Snappy](http://code.google.com/p/snappy-java/) for high-speed de/compression.

##Usage

Include in a Leiningen project by adding this to your dependencies:

```clojure
[deep-freeze "1.2.2-SNAPSHOT"]
```

Simply **:require deep-freeze.core** and then use the 4 core functions to serialize/deserialize data.

###(freeze-to-stream item stream)
Serializes the item into the java OutputStream.

###(freeze-to-array item)
Serializes the item into a byte array.

###(thaw stream)
Deserializes a clojure structure form the java InputStream.

###(thaw-from-array array)
Same as thaw but uses a byte array as input.

## License (BSD)

Copyright (c) 2011, Timothy Baldridge
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

Neither the name of the <ORGANIZATION> nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.