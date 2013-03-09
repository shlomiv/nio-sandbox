(ns vxn.test
  (:import
     [java.net InetSocketAddress]
     [java.nio ByteBuffer CharBuffer]
     [java.nio.channels ServerSocketChannel Selector SelectionKey]
     [java.nio.charset Charset])
  ;(:use index-server.core)
  )

(def pool (atom (mapv (fn [_] (ByteBuffer/allocateDirect 1000)) (range 10))))

(defn borrow-buffer []
  (let [b @pool]
    (if (compare-and-set! pool b (rest b))
      (first b)
      (recur)))

(defn return-buffer [b]
  (swap! pool conj b))

(defn selector [server-socket-channel]
  (let [selector (Selector/open)]
    (.register server-socket-channel selector SelectionKey/OP_ACCEPT)
    selector))

(defn setup [port]
  (let [server-socket-channel (ServerSocketChannel/open)
        _ (.configureBlocking server-socket-channel false)
        server-socket (.socket server-socket-channel)
        inet-socket-address (InetSocketAddress. port)]
    (.bind server-socket inet-socket-address)
    [(selector server-socket-channel)  server-socket]))

(defn state= [state channel]
  (= (bit-and (.readyOps channel) state) state))

(defn accept-connection [server-socket selector]
  (let [channel (-> server-socket (.accept) (.getChannel))]
    (println "Connection from" channel)
    (doto channel
      (.configureBlocking false)
      (.register selector SelectionKey/OP_READ))))

(defn read-remaining [sc b]
  (.read sc b)
  (if (.hasRemaining b)
    (recur sc b)
    b))

(defn read-socket [selected-key f e]
  (let [socket-channel (.channel selected-key)
        buffer (or (.attachment selected-key)
                   ((let [b (borrow-buffer)] (.attach selected-key b) b)))]
    (when (zero? (.position b))
      (let [msg-length  (-> (.limit 4 buffer)  ;beginning of first message
                            (read-remaining socket-channel)
                            (.flip)
                            (.getInt))]
        (if (> (.capacity buffer) (+ 4 msg-length)) ; we have enough room to process message
          (println "buffer too small message of length" msg-length (.capacity buffer) (.close socket-channel))
          (.limit buffer msg-length))))
    (.read socket-channel buffer)

    (when-not (.hasRemaining buffer)
      (future (f buffer socket-channel) (return-buffer buffer)))

    (if (= (.limit *buffer*) 0)
      (do
        (println "Lost connection from" socket-channel)
        (.cancel selected-key)
        (.close (.socket socket-channel))
        (e socket-channel))
      (do (println "GOT " (Thread/currentThread))
          (.write socket-channel (ByteBuffer/wrap (.getBytes "ass"))#_(f *buffer*))))))

(def run-server (agent true))

(defn react [selector server-socket f e]
  (while @run-server
    (.println System/out "AAAA")
    (when (pos? (.select selector 1000))
      (let [selected-keys (.selectedKeys selector)]
        (doseq [k selected-keys]
          (condp state= k
            SelectionKey/OP_ACCEPT (accept-connection server-socket selector)
            SelectionKey/OP_READ   (read-socket k f e)))
        (.clear selected-keys)))))


(defn run []
  (let [[sel socket] (setup 2323)
        t (Thread. (fn [] (react sel socket identity #())))]
    (send run-server (fn [x] true))
    (.start t)
    (fn []
      (do
        (print "A")
        (send run-server (fn [x] false))
        (.interrupt t)
        (.close socket)
        (.close sel)))))

(def a (run))
(a)




;(print "a")
;(type a)

(defn buffer->string
  ([byte-buffer]
   (buffer->string byte-buffer (Charset/defaultCharset)))
  ([byte-buffer charset]
   (.toString (.decode charset byte-buffer))))

(defn string->buffer
  ([string]
   (string->buffer string (Charset/defaultCharset)))
  ([string charset]
   (.encode charset string)))
