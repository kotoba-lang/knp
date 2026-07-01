(ns knp-test
  (:require [clojure.test :refer [deftest is testing]]
            [knp]))
(deftest namespace-loads
  (testing "the restored CLJC namespace loads"
    (is (some? knp))))
