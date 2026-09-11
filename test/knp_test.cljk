(ns knp-test
  (:require [clojure.test :refer [deftest is testing]]
            [knp]
            [knp.packet]
            [knp.channel]
            [knp.session]))

(deftest namespace-loads
  (testing "the restored root CLJC namespace loads and re-exports the portable surface"
    (is (some? knp/make-header))
    (is (some? knp/channel-manager))
    (is (some? knp/session-manager))))
