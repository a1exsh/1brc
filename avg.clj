(ns avg
  (:require [clojure.core.reducers :as r]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clj-async-profiler.core :as prof]))

(defn parse-temp-long
  "Takes a temperature reading in the format `12.3` and returns a `long` of
  decigrads, e.g. `123`."
  [s]
  #_(-> s (string/replace-first "." "") parse-long)
  (reduce (fn [^long acc c]
            (case c
              \. acc
              (+ (* 10 acc)
                 (- (int c) (int \0)))))
          0 s))
#_
(->> "12.3" (reduce (fn [acc c] (case c \. acc (+ (* 10 acc) (- (int c) (int \0))))) 0))

(defn temp-str
  "Takes temperature in decigrads, e.g. `123`, and returns a string like
  `12.3`."
  [dg]
  (format "%.1f" (/ dg 10.0)))

(defprotocol TempAggregator
  (agg-temp [this temp]))

(deftype Station [^long min-t ^long max-t ^long sum-t ^int cnt-t]
  TempAggregator
  (agg-temp [this temp]
    (Station. (min min-t temp)
              (max max-t temp)
              (+   sum-t temp)
              (inc cnt-t))))

(defn make-station [temp]
  (Station. temp temp temp 1))

(defn report-avgs [m]
  (str "{"
       (->> m
            (map (fn [[city stat]]
                   (str (name city)
                        "="
                        (->> [(.min-t stat)
                              (/ (.sum-t stat) (.cnt-t stat))
                              (.max-t stat)]
                             (map temp-str)
                             (string/join "/")))))
            (string/join ", "))
       "}"))


(defn agg-aggs
  [& aggs]
  (apply merge-with
         (fn [s1 s2]
           (Station. (min (.min-t s1) (.min-t s2))
                     (max (.max-t s1) (.max-t s2))
                     (+   (.sum-t s1) (.sum-t s2))
                     (+   (.cnt-t s1) (.cnt-t s2))))
         aggs))

(defn agg-batch
  [n lines]
  (r/fold n
          agg-aggs
          (fn [stations line]
            (let [semi-index   (string/index-of line \;)
                  station-name (keyword (subs line 0 semi-index))
                  temp-s       (subs line (inc semi-index))
                  temp         (parse-temp-long temp-s)]
              (update stations
                      station-name
                      (fn [stat]
                        (if stat
                          (agg-temp stat temp)
                          (make-station temp))))))
          lines))

(time
 (def input
   (with-open [rdr (io/reader "measurements2.txt")]
     (->> rdr
          line-seq
          (take 1000000)
          vec))))

(time
 (binding [*unchecked-math* true]
   (with-open [rdr (io/reader "measurements2.txt")]
     (->> rdr
          line-seq
          (take 100000000)

          (partition-all 1000000)
          (map vec)
          
          (map #(agg-batch 4096 %))
          agg-aggs
          #_(agg-batch 65536)

          (into (sorted-map))
          report-avgs))))

(prof/serve-ui 8080)

(time
 (prof/profile
  (binding [*unchecked-math* true]
    (with-open [rdr (io/reader "measurements2.txt")]
      (->> rdr
           line-seq
           (take 10000000)

           (partition-all 100000)
           (map vec)
           (map agg-batch)
           agg-aggs

           (into (sorted-map))
           report-avgs)))))

(prof/generate-diffgraph 6 7 {})


(comment
  (time
   (binding [*unchecked-math* false]      ; didn't make a difference
     (let [results-chan (async/chan)]
       (with-open [rdr (io/reader "measurements2.txt")]
         (->> rdr
              line-seq
              (take 1000000)

              (reduce (fn [stations line]
                        (let [semi-index   (string/index-of line \;)
                              station-name (subs line 0 semi-index)
                              temp-s       (subs line (inc semi-index))
                              temp         (parse-temp-long temp-s)
                              stat-chan    (get stations station-name)]
                          (if stat-chan
                            (do
                              (async/>!! stat-chan temp)
                              stations)
                            (let [stat-chan (async/chan)]
                              (async/go-loop [stat (make-station temp)]
                                (if-let [temp (async/<! stat-chan)]
                                  (recur (agg-temp stat temp))
                                  (async/>! results-chan [station-name stat])))
                              (assoc stations station-name stat-chan)))))
                      {})
              vals
              (map async/close!)
              dorun))
       (async/close! results-chan)

       (->> results-chan
            (async/into (sorted-map))
            async/<!!
            report-avgs)))))


(comment
  (time
   (binding [*unchecked-math* false]    ; didn't make a difference
     (with-open [rdr (io/reader "measurements.txt")]
       (->> rdr
            line-seq
            (take 1000000)

            (reduce (fn [stations line]
                      (let [semi-index   (string/index-of line \;)
                            station-name (subs line 0 semi-index)
                            temp-s       (subs line (inc semi-index))
                            temp         (parse-temp-long temp-s)
                            stat         (get stations station-name)]
                        (if stat
                          (do
                            (send stat agg-temp temp)
                            stations)
                          (let [stat (agent (make-station temp))]
                            (assoc stations station-name stat)))))
                    {})
            (map (fn [[s a]]            ; good luck making sure that agents
                   [s @a]))             ; have done all the work...

            (into (sorted-map))
            report-avgs

            #_dorun)))))
