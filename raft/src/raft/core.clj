(ns raft.core
  "Write your code here."
  (:require [clojure.core.async :as async]
            [go.time :as time]
            [lab-rpc.core :as lab-rpc]
            [raft.persister :as persister]
            [taoensso.timbre :as timbre]))


(def ^:const election-timeout-range-ms
  "Min 250ms and max 500ms"
  [250 250])

(def ^:const heartbeat-timeout
  (-> election-timeout-range-ms
      first
      (/ 2)))

(def ^:const tick
  "Interval ms at which to check the timers"
  10)


(timbre/set-level! :info)


(defn gen-election-timeout
  []
  (+ (first election-timeout-range-ms)
     (-> election-timeout-range-ms
         second
         rand-int)))


(defn since-ms
  "Time passed in ms since `s` till function invocation."
  [s]
  (-> (System/nanoTime)
      (- s)
      double
      (/ 1e6)))


(defprotocol IRaft
  (RequestVote [this [term candidate-id last-log-ix last-log-term]])
  (AppendEntries [this [term leader-id prev-log-ix prev-log-term entries leader-commit]]))


(defrecord Raft [me peers persister state ops-chan]
  IRaft
  (RequestVote [this [term candidate-id last-log-ix last-log-term]]
    (let [reply-chan (async/chan)]
      (async/>!! ops-chan {:op       :request-vote
                           :args     [term candidate-id last-log-ix last-log-term]
                           :reply-on reply-chan})
      (async/<!! reply-chan)))
  (AppendEntries [this [term leader-id prev-log-ix prev-log-term entries leader-commit]]
    (let [reply-chan (async/chan)]
      (async/>!! ops-chan {:op       :append-entries
                           :args     [term leader-id prev-log-ix
                                      prev-log-term entries leader-commit]
                           :reply-on reply-chan})
      (async/<!! reply-chan))))


(defn handle-request-vote
  [raft {:keys [args reply-on]}]
  (timbre/debugf "me:%s handle-request-vote" (:me raft))
  (let [[term candidate-id
         last-log-ix last-log-term] args
        {:keys [current-term voted-for
                log-ix log-term]}   (-> raft :state deref)
        voted-for*                  (if (> term current-term)
                                      nil ;; Not voted in the new term
                                      voted-for)
        current-term*               (if (> term current-term)
                                      term
                                      current-term)
        grant-vote?                 (and (not (< term current-term))
                                         (or (nil? voted-for*)
                                             (= candidate-id voted-for*))
                                         (or (< log-term last-log-term)
                                             (and (= log-term last-log-term)
                                                  (<= log-ix last-log-ix))))]
    (if grant-vote?
      (do
        (timbre/debugf "me:%s voting for candidate:%s" (:me raft) candidate-id)
        (vector {:role            :follower
                 :votes-received  0
                 :voted-for       candidate-id
                 :current-term    current-term*
                 :timer           (time/now)
                 :election-timout (gen-election-timeout)}
                [(fn [_] (async/>!! reply-on [current-term* grant-vote?]))]))
      (do
        ;; Reply with current-term (not *) so that the vote requester knows that
        ;; this instance has a term which is in future, and hence the vote
        ;; requester must convert to a follower.
        (timbre/infof "me:%s not voting for candidate:%s" (:me raft) candidate-id)
        (vector nil [(fn [_] (async/>!! reply-on [current-term grant-vote?]))])))))


(defn handle-append-entries
  [raft {:keys [args reply-on]}]
  (timbre/debugf "me:%s handle-append-entries" (:me raft))
  (let [[term leader-id prev-log-ix
         prev-log-term entries leader-commit] args
        state                                 (-> raft :state deref)
        current-term                          (:current-term state)
        voted-for                             (:voted-for state)
        log                                   (:log state)
        current-term*                         (if (> term current-term)
                                                term
                                                current-term)
        voted-for*                            (if (> term current-term)
                                                nil
                                                voted-for)
        prev-entry                            (get log prev-log-ix)
        valid-prev-entry?                     (and (not (< term current-term*))
                                                   (= (:term prev-entry) prev-log-term))
        ;; FIXME Check here for a conflicting entry (point 4 in AppendEntries RPC)
        ;; FIXME leadercommit logic here point 5 here
        ]
    (if valid-prev-entry?
      (do
        (timbre/debugf "me:%s Previous entry is valid." (:me raft))
        (when (seq entries)
          ;; TODO Actually append entries to the log
          nil)
        (vector {:role             :follower
                 :votes-received   0
                 :voted-for        voted-for*
                 :current-term     current-term*
                 :timer            (time/now)
                 :election-timeout (gen-election-timeout)}
                [(fn [_] (async/>!! reply-on [current-term* valid-prev-entry?]))]))
      (do
        (timbre/debugf "me:%s Previous entry is NOT valid." (:me raft))
        (vector nil
                [(fn [_] (async/>!! reply-on [current-term valid-prev-entry?]))])))))


;; Only handling heartbeats for now
(defn send-append-entries
  [raft]
  (let [state          (-> raft :state deref)
        peers          (:peers raft)
        term           (:current-term state)
        leader-id      (:me raft)
        ops-chan       (:ops-chan raft)
        prev-log-index (:log-ix state)
        prev-log-term  (:log-term state)
        entries        []
        leader-commit  (:commit-ix state)]
    (doseq [peer peers]
      (async/thread
        (when-let [response (lab-rpc/call!
                             peer
                             :Raft/AppendEntries
                             [term leader-id prev-log-index prev-log-term entries leader-commit])]
          (async/>!! ops-chan {:op   :received-append-entries-response
                               :args response}))))))


(defn handle-append-entries-response
  [raft {:keys [args]}]
  (let [[term success?] args
        state           (-> raft :state deref)
        current-term    (:current-term state)]
    (if (< term current-term)
      (do
        (timbre/info "me:%s Received AppendEntries Response for old term:%s" (:me raft) term)
        (vector nil []))
      (if (> term current-term)
        (do
          (timbre/infof "me:%s Saw higher term:%s than current:%s. Converting to follower!"
                        (:me raft) term current-term)
          (vector {:role             :follower
                   :votes-recevied   0
                   :voted-for        nil
                   :current-term     term
                   :timer            (time/now)
                   :election-timeout (gen-election-timeout)}
                  []))
        (if success?
          ;; TODO Handle the `success` param, check if majority of the followers
          ;; have accepted the entry
          (vector nil [])
          (vector nil []))))))


(defn request-vote-from-peers
  "Only called when voted for self first. Hence, majority = peers/2."
  [{:keys [peers ops-chan me] :as raft}]
  (timbre/debugf "me:%s request-vote-from-peers" (:me raft))
  (let [state         (-> raft :state deref)
        term          (:current-term state)
        last-log-ix   (:log-ix state)
        last-log-term (:log-term state)]
    (doseq [peer peers]
      (async/thread
        (when-let [response (lab-rpc/call! peer :Raft/RequestVote [term me last-log-ix last-log-term])]
          (async/>!! ops-chan {:op   :received-vote
                               :args response}))))))


(defn start-election
  "Refer to Figure 2 -> Rules for Servers -> Candidates -> 'On conversion to
  candidate, start election'"
  [raft]
  ;; TODO FIXME Dedupe votes. A follower may vote for you multiple times
  (timbre/infof "me:%s start-election" (:me raft))
  (vector {:role             :candidate
           :current-term     (-> raft
                               :state
                               deref
                               :current-term
                               inc)
           :voted-for        (:me raft)
           :votes-received   1
           :timer            (time/now)
           :election-timeout (gen-election-timeout)}
          [request-vote-from-peers]))


;; FIXME Rewrite this fn
(defn handle-vote
  "Refer to Figure 4.
  A candidate must have voted for itself. Hence, the majority of the cluster is (- N 1)/2."
  [raft {:keys [args]}]
  (timbre/debugf "me:%s handle-vote" (:me raft))
  (let [[term vote-granted?] args
        state                (-> raft :state deref)
        current-term         (:current-term state)]
    (if (< term current-term)
      (do
        (timbre/info "me:%s Received vote for old term:%s with value:%s" (:me raft) term vote-granted?)
        (vector nil []))
      (if (> term current-term)
        (do
          (timbre/infof "me:%s Saw higher term:%s than current-term:%s. Converting to follower"
                        (:me raft) term current-term)
          (vector {:role             :follower
                   :votes-received   0
                   :voted-for        nil
                   :current-term     term
                   :timer            (time/now)
                   :election-timeout (gen-election-timeout)}
                  []))
        (if (and vote-granted? (not= :leader (:role state)))
          (let [votes-received (-> state :votes-received inc)
                peers          (:peers raft)
                majority?      (>= votes-received
                                   (-> peers count (/ 2)))
                log-ix         (:log-ix state)
                new-state      (merge {:votes-received votes-received}
                                      (when majority?
                                        (timbre/infof "me:%s I am the leader now with current-term:%s"
                                                      (:me raft) current-term)
                                        {:role     :leader
                                         :timer    (time/now)
                                         :next-ix  (-> peers
                                                       count
                                                       (repeatedly #(inc log-ix))
                                                       vec)
                                         :match-ix (-> peers
                                                       count
                                                       (repeatedly (constantly 0))
                                                       vec)}))
                effect         (if majority? [send-append-entries] [])]
            (vector new-state effect))
          (vector nil []))))))


(defn transition-state!
  [raft delta]
  (timbre/debugf "me:%s transition-state!" (:me raft))
  (swap! (:state raft) merge delta)
  [])


(defn run-effects!
  [raft effects]
  (timbre/debugf "me:%s run-effects!" (:me raft))
  (doseq [effect effects]
    (effect raft)))


(defn pong
  [raft]
  (timbre/infof "me:%s pong!" (:me raft))
  [nil []])


(defn handle!
  [raft {:keys [op args reply-on]
         :as   operation}]
  (let [role                  (-> raft :state deref :role)
        [state-delta effects] (case [role op]
                                ([:follower  :ping]
                                 [:candidate :ping]
                                 [:leader    :ping])
                                (pong raft)

                                ([:follower  :election-timeout]
                                 [:candidate :election-timeout])
                                (start-election raft)

                                ([:follower  :request-vote]
                                 [:candidate :request-vote]
                                 [:leader    :request-vote])
                                (handle-request-vote raft operation)

                                ([:follower  :append-entries]
                                 [:candidate :append-entries]
                                 [:leader    :append-entries])
                                (handle-append-entries raft operation)

                                ([:candidate :received-vote]
                                 [:leader    :received-vote])
                                (handle-vote raft operation)

                                [:leader :send-heartbeat]
                                (send-append-entries raft)

                                ([:follower  :received-append-entries-response]
                                 [:candidate :received-append-entries-response]
                                 [:leader    :received-append-entries-response])
                                (handle-append-entries-response raft operation))
        ;; TODO FIXME
        ;; In additional-effects, check whether the term and commit index are correct.
        ;; Refer to rules for ALL servers
        additional-effects (transition-state! raft state-delta)
        effects            (concat effects additional-effects)]
    (run-effects! raft effects)
    raft))


;; TODO Add some documentation for the custom ks.
(defn raft
  [peers me persister ops-chan]
  (let [state (atom {:role             :follower
                     :current-term     0
                     :voted-for        nil
                     :votes-received   0 ;; Valid votes recevied after transition to leader are not counted
                     :leader-id        nil
                     :log              [{:term 0}] ;; Placeholder log entry
                     :log-ix           0
                     :log-term         0
                     :commit-ix        0
                     :last-applied-ix  0
                     :next-ix          nil
                     :match-ix         nil
                     :timer            (time/now)
                     :election-timeout (gen-election-timeout)})]
    (->Raft me
            peers
            persister
            state
            ops-chan)))


(defn get-state
  "Get the current state for the Raft instance."
  [raft]
  (let [state   (-> raft
                    :state
                    deref)
        term    (:current-term state)
        leader? (= :leader (:role state))]
    [term leader?]))


(defn persist
  "Save Raft's persistent state to stable storage,
  where it can later be retrieved after a crash and restart.
  See paper's Figure 2 for a description of what should be persistent."
  [raft]
  ;; Your code here (2C).
  )


(defn read-persist
  "Restore previously persisted state."
  [raft data]
  ;; Your code here (2C).
  )

;; example code to send a RequestVote RPC to a server.
;; server is the index of the target server in rf.peers[].
;; expects RPC arguments in args.
;;
;; The lab-rpc package simulates a lossy network, in which servers
;; may be unreachable, and in which requests and replies may be lost.
;; call! sends a request and waits for a reply. If a reply arrives
;; within a timeout interval, call! returns the result; otherwise
;; call! returns nil. Thus call! may not return for a while.
;; A nil return can be caused by a dead server, a live server that
;; can't be reached, a lost request, or a lost reply.
;;
;; call! is guaranteed to return (perhaps after a delay) *except* if the
;; handler function on the server side does not return. Thus there
;; is no need to implement your own timeouts around call!.
;;
;; look at the comments in lab-rpc/src/lab_rpc/core.clj for more details.
(defn send-request-vote
  [raft server args]
  (let [peers (:peers raft)]
    (lab-rpc/call! (nth peers server) :Raft/RequestVote args)))

;; the service using Raft (e.g. a k/v server) wants to start
;; agreement on the next command to be appended to Raft's log. if this
;; server isn't the leader, returns false. otherwise start the
;; agreement and return immediately. there is no guarantee that this
;; command will ever be committed to the Raft log, since the leader
;; may fail or lose an election. even if the Raft instance has been killed,
;; this function should return gracefully.
;;
;; the first return value is the index that the command will appear at
;; if it's ever committed. the second return value is the current
;; term. the third return value is true if this server believes it is
;; the leader.
(defn start
  "Start agreement on the next command."
  [raft command]
  (let [index   nil
        term    nil
        leader? true]
    ;; Your code here (2B).
    [index term leader?]))


(defn kill
  "Kill this Raft instance."
  [raft]
  ;; SAVE YOUR PERSISTENT STATE
  ;; Close any reply chans
  ;; Close timer chans
  ;; Stop the event loop
  (timbre/infof "me:%s Shutting down instance!" (:me raft))
  (async/close! (:ops-chan raft))
  nil)

;; the service or tester wants to create a Raft server. the ports
;; of all the Raft servers (including this one) are in peers[]. this
;; server's port is peers[me]. all the servers' peers[] arrays
;; have the same order. persister is a place for this server to
;; save its persistent state, and also initially holds the most
;; recent saved state, if any. applyCh is a channel on which the
;; tester or service expects Raft to send ApplyMsg messages.
;; Make() must return quickly, so it should start goroutines (or threads)
;; for any long-running work.
(defn make
  [peers me persister apply-ch]
  (timbre/infof "me:%s Starting up" me)
  (let [peers-without-me (-> peers
                             vec
                             (subvec 0 me)
                             (concat (subvec (vec peers) (inc me)))
                             vec)
        ops-chan         (async/chan)
        raft             (raft peers-without-me
                               me
                               persister
                               ops-chan)]
    ;; Your initialization code here (2A, 2B, 2C).
    ;; initialize from state persisted before a crash

    ;; DEBUG
    (comment
      (async/thread
        (timbre/infof "me:%s ping!" (:me raft))
        (async/>!! ops-chan {:op :ping})))

    ;; Timer thread
    ;; Similar to go.time/after-func
    (async/thread
      (loop []
        (async/<!! (async/timeout tick))
        (let [state            (-> raft :state deref)
              timer            (:timer state)
              election-timeout (:election-timeout state)]
          (if (= :leader (:role state))
            (when (> (since-ms timer) heartbeat-timeout)
              (async/>!! ops-chan {:op :send-heartbeat}))
            (when (> (since-ms timer) election-timeout)
              (timbre/infof "me:%s Election timeout!" (:me raft))
              (async/>!! ops-chan {:op :election-timeout})))
          (recur))))

    ;; Event loop thread
    (async/thread
      (loop [operation (async/<!! ops-chan)]
        (if (some? operation)
          (do
            (timbre/debugf "me:%s Handling op:%s" (:me raft) (:op operation))
            (handle! raft operation)
            (recur (async/<!! ops-chan)))
          (timbre/infof "me:%s Shutting down event loop!" (:me raft)))))

    ;; Create persister
    (read-persist raft (persister/read-raft-state persister))
    raft))
