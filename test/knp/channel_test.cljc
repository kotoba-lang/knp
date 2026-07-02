(ns knp.channel-test
  "kami-knp/src/channel.rs had no `#[cfg(test)]` block in the original Rust
  (channel.rs shipped without unit tests). These are new coverage for the
  ported CLJC state machine, exercising the same behavior the Rust doc
  comments describe (per-channel sequencing, peer-ack bookkeeping,
  retransmit scheduling)."
  (:require [clojure.test :refer [deftest is testing]]
            [knp.channel :as channel]))

(deftest send-increments-per-channel-sequence
  (testing "each channel keeps its own independent sequence counter"
    (let [cm (channel/channel-manager)
          [b1 cm2] (channel/send cm :unreliable #?(:clj (byte-array [1])
                                                     :cljs (js/Uint8Array. #js [1])))
          [b2 cm3] (channel/send cm2 :unreliable #?(:clj (byte-array [2])
                                                      :cljs (js/Uint8Array. #js [2])))
          [b3 _cm4] (channel/send cm3 :reliable-ordered #?(:clj (byte-array [3])
                                                             :cljs (js/Uint8Array. #js [3])))]
      (is (not= (vec b1) (vec b2)))
      ;; reliable-ordered starts its own counter at 0, independent of unreliable's count
      (let [[[ch1 _] _] (channel/receive (channel/channel-manager) b1)]
        (is (= :unreliable ch1)))
      (let [[[ch3 _] _] (channel/receive (channel/channel-manager) b3)]
        (is (= :reliable-ordered ch3))))))

(deftest send-receive-roundtrip
  (testing "send then receive recovers channel and payload"
    (let [cm (channel/channel-manager)
          payload #?(:clj (byte-array [9 8 7]) :cljs (js/Uint8Array. #js [9 8 7]))
          [wire cm2] (channel/send cm :voice payload)
          [[ch recv-payload] cm3] (channel/receive cm2 wire)]
      (is (= :voice ch))
      (is (= [9 8 7] (vec recv-payload)))
      ;; receiving updates peer-ack for that channel to the received sequence
      (is (= 0 (get (:peer-ack cm3) :voice))))))

(deftest receive-garbage-returns-nil
  (testing "too-short bytes fail to parse without throwing"
    (let [cm (channel/channel-manager)
          [result cm2] (channel/receive cm #?(:clj (byte-array [1])
                                                :cljs (js/Uint8Array. #js [1])))]
      (is (nil? result))
      (is (= cm cm2)))))

(deftest get-retransmits-empty-when-nothing-unacked
  (testing "no unacked packets means no retransmits"
    (let [cm (channel/channel-manager)
          [result _cm2] (channel/get-retransmits cm 1000)]
      (is (= [] result)))))
