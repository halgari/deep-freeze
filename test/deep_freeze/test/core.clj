(ns deep-freeze.test.core
  (:use [deep-freeze.core] :reload)
  (:use [clojure.test]))

(defrecord testrecord [name age])
(defrecord testtype   [name age])

(def testdata
  "Equality should hold for these elements pre/post serialization."

  {:integer    (int 3)
   :long       (long 3)
   :float      (float 3.14)
   :double     (double 3.14)
   :bigint     (bigint 31415926535897932384626433832795)
   :bigdec     (bigdec 3.1415926535897932384626433832795)
   :ratio      22/7
   :boolean    true
   :char       \ಬ
   :string     "ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ"
   :key        :key
   ;;:list       (list 1 2 3 4 5 (list 6 7 8 (list 9 10)))
   :list       '(1 2 3 4 5 (6 7 8 (9 10)))
   :vector     [1 2 3 4 5 [6 7 8 [9 10]]]
   :set        #{1 2 3 4 5 #{6 7 8 #{9 10}}}
   :map        {:a 1 :b 2 :c 3 :d {:e 4 :f {:g 5 :h 6 :i 7}}}
   :empty-vec  []
   :empty-set  #{}
   :empty-list (list)
   :meta       (with-meta {:a :A} {:metakey :metaval})
   :nil        nil

   ;; TODO Still to implement support for these
   ;;:type       (testtype. "Stu" 26)
   ;;:record     (testrecord. "Stu" 26)
   ;;:function   (fn [x] (* x x))
   ;;:closure    (let [x 2] (fn [y] (* x y)))
   })

(defn array-roundtrip [item] (thaw-from-array (freeze-to-array item)))

(deftest test-arrays
  (is (= testdata (array-roundtrip testdata)) "Preserves standard datatypes")

  (is (let [t (with-meta {:a :A} {:metakey :metaval})]
        (= (meta t) (meta (array-roundtrip t)))) "Preserves metadata")

  ;; Equality needs to be munged for STM types
  (is (let [t (atom  "atom")]  (= @t @(array-roundtrip t)))  "Preserves atoms")
  (is (let [t (ref   "ref")]   (= @t @(array-roundtrip t)))  "Preserves refs")
  (is (let [t (agent "agent")] (= @t @(array-roundtrip t)))  "Preserves agents")

  (is (let [t (.getBytes "Hi!")]
        (= (String. t) (String. (array-roundtrip t)))) "Preserves byte[]s"))

(deftest test-streaming
  (is false "TODO"))