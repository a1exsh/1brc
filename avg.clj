(ns avg
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.string :as string]))

(defn parse-temp-long
  "Takes a temperature reading in the format `12.3` and returns a `long` of
  decigrads, e.g. `123`."
  [s]
  (-> s (string/replace-first "." "") parse-long))

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
                   (str city "=" (->> [(.min-t stat)
                                       (/ (.sum-t stat) (.cnt-t stat))
                                       (.max-t stat)]
                                      (map temp-str)
                                      (string/join "/")))))
            (string/join ", "))
       "}"))

(time
 (binding [*unchecked-math* false]      ; didn't make a difference
   (let [results-chan (async/chan)]
     (with-open [rdr (io/reader "measurements.txt")]
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
          report-avgs))))


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
            (map (fn [[s a]]
                   [s @a]))
            (into (sorted-map))
            report-avgs

            #_dorun)))))
