(ns knp.channel
  "KNP channel manager: per-channel sequence numbers, peer-ACK bookkeeping,
  and retransmit scheduling — the pure state-machine half of channel
  handling.

  Restored from the legacy kami-engine/kami-knp Rust crate (deleted in PR
  #82, path `kami-knp/src/channel.rs`) as part of ADR-2607010930
  (com-junkawasaki/root, kami-engine Rust-workspace restoration to CLJC).

  Ported 1:1: `ReliableChannel` (send/recv sequence counters, unacked-packet
  bookkeeping) and `ChannelManager` (`send`, `receive`, `get_retransmits`),
  all of which are pure data + functions over that data — none of it touches
  a socket. `channel.rs` itself performs no I/O, so nothing from this
  specific file was excluded; wire encode/decode is delegated to
  `knp.packet`, and actual UDP transmission stays in the (unported)
  `socket.rs` / `client.rs` / `server.rs`."
  (:refer-clojure :exclude [send])
  (:require [knp.packet :as packet]))

(def send-buffer-size 256)
(def ack-timeout-ms 100)

;; ---------------------------------------------------------------------------
;; ReliableChannel: per-channel send/recv sequence + unacked ring buffer

(defn reliable-channel
  "New reliable-channel state. `unacked` is a sparse map keyed by
  `seq mod send-buffer-size` (CLJC analogue of the Rust fixed-size
  `[Option<UnackedPacket>; SEND_BUFFER_SIZE]` ring buffer)."
  []
  {:send-seq 0
   :recv-seq 0
   :unacked {}})

(defn reliable-channel-next-seq
  "Returns [next-seq updated-channel]. Sequence wraps at 16 bits, matching
  Rust's `u16::wrapping_add`."
  [rc]
  (let [seq (:send-seq rc)]
    [seq (update rc :send-seq (fn [s] (bit-and (inc s) 0xFFFF)))]))

;; ---------------------------------------------------------------------------
;; ChannelManager: all 4 KNP channels

(defn channel-manager
  "New channel-manager state covering all 4 KNP channels."
  []
  {:reliable-ordered (reliable-channel)
   :reliable-unordered (reliable-channel)
   :unreliable-seq 0
   :voice-seq 0
   ;; latest ACK per channel from peer, indexed by channel keyword
   :peer-ack {:unreliable 0 :reliable-ordered 0 :reliable-unordered 0 :voice 0}})

(defn- wrap16 [n] (bit-and n 0xFFFF))

(defn send
  "Prepare a packet for sending on `channel`. Returns
  [wire-bytes updated-manager]."
  [cm channel payload]
  (let [[seq flags cm]
        (case channel
          :unreliable
          [(:unreliable-seq cm) #{} (update cm :unreliable-seq (comp wrap16 inc))]

          :reliable-ordered
          (let [[seq rc'] (reliable-channel-next-seq (:reliable-ordered cm))]
            [seq #{:reliable :ordered} (assoc cm :reliable-ordered rc')])

          :reliable-unordered
          (let [[seq rc'] (reliable-channel-next-seq (:reliable-unordered cm))]
            [seq #{:reliable} (assoc cm :reliable-unordered rc')])

          :voice
          [(:voice-seq cm) #{} (update cm :voice-seq (comp wrap16 inc))])

        ack (get (:peer-ack cm) channel 0)
        pkt (packet/make-packet channel flags seq ack payload)]
    [(packet/packet->bytes pkt) cm]))

(defn receive
  "Process a received wire packet. Returns [[channel payload] updated-manager]
  or [nil updated-manager] if the bytes didn't parse."
  [cm bytes]
  (if-let [pkt (packet/bytes->packet bytes)]
    (let [channel (packet/header-channel (:header pkt))
          seq (packet/header-sequence (:header pkt))
          cm' (assoc-in cm [:peer-ack channel] seq)]
      [[channel (:payload pkt)] cm'])
    [nil cm]))

(defn get-retransmits
  "Packets on the reliable-ordered channel whose `sent-at-ms` is older than
  `ack-timeout-ms` relative to `now-ms`. Returns [wire-bytes-seq updated-manager]
  with `sent-at-ms`/`retransmit-count` bumped for the ones returned (mirrors
  the Rust loop over the unacked ring buffer)."
  [cm now-ms]
  (let [rc (:reliable-ordered cm)
        due? (fn [[_ up]] (> (- now-ms (:sent-at-ms up)) ack-timeout-ms))
        due (filter due? (:unacked rc))
        result (mapv (fn [[_ up]] (:data up)) due)
        unacked' (reduce (fn [m [seq up]]
                            (assoc m seq (-> up
                                              (assoc :sent-at-ms now-ms)
                                              (update :retransmit-count inc))))
                          (:unacked rc)
                          due)
        cm' (assoc cm :reliable-ordered (assoc rc :unacked unacked'))]
    [result cm']))
