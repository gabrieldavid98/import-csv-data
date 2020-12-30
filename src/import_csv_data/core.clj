(ns import-csv-data.core
  (:require 
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [semantic-csv.core :as sc])
  (:gen-class))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))

(def csv "resources/match_scores_1968-1990_unindexed_csv.csv")

(defn first-match [csv]
  (with-open [r (io/reader csv)]
    (->> (csv/read-csv r)
         sc/mappify
         first)))

(defn five-matches [csv]
  (with-open [r (io/reader csv)]
    (->> (csv/read-csv r)
         sc/mappify
         (map #(select-keys % [:tourney_year_id
                               :winner_name
                               :loser_name
                               :winner_sets_won
                               :loser_sets_won]))
         (take 5)
         doall)))

(defn five-matches-int-sets [csv]
  (with-open [r (io/reader csv)]
    (->> (csv/read-csv r)
         sc/mappify
         (map #(select-keys % [:tourney_year_id
                               :winner_name
                               :loser_name
                               :winner_sets_won
                               :loser_sets_won]))
         (sc/cast-with {:winner_sets_won sc/->int
                        :loser_sets_won sc/->int})
         (take 5)
         doall)))

(defn match-query [csv pred]
  (with-open [r (io/reader csv)]
    (->> (csv/read-csv r)
         sc/mappify
         (sc/cast-with {:winner_sets_won sc/->int
                        :loser_sets_won sc/->int
                        :winner_games_won sc/->int
                        :loser_games_won sc/->int})
         (filter pred)
         (map #(select-keys % [:winner_name
                               :loser_name
                               :winner_sets_won
                               :loser_sets_won
                               :winner_games_won
                               :loser_games_won
                               :tourney_year_id
                               :tourney_slug]))
         doall)))

(defn rivalry-data [csv player-1 player-2]
  (with-open [r (io/reader csv)]
    (let [data (->> (csv/read-csv r)
                    sc/mappify
                    (sc/cast-with {:winner_sets_won sc/->int
                                   :loser_sets_won sc/->int
                                   :winner_games_won sc/->int
                                   :loser_games_won sc/->int})
                    (filter #(= (hash-set (:winner_name %) (:loser_name %))
                                #{player-1 player-2}))
                    (map #(select-keys % [:winner_name
                                          :loser_name
                                          :winner_sets_won
                                          :loser_sets_won
                                          :winner_games_won
                                          :loser_games_won
                                          :tourney_year_id
                                          :tourney_slug])))
          victories-per-player #(filter (comp #{%} :winner_name) data)]
      {:first-victory-player-1 (first (victories-per-player player-1))
       :first-victory-player-2 (first (victories-per-player player-2))
       :total-matches (count data)
       :total-victories-player-1 (count (victories-per-player player-1))
       :total-victories-player-2 (count (victories-per-player player-2))
       :most-competitive-matches (->> data
                                      (filter #(= 1 (- (:winner_sets_won %) 
                                                       (:loser_sets_won %)))))})))

;; {:tourney_slug "adelaide",
;;  :loser_slug "michael-stich",
;;  :winner_sets_won "2",
;;  :match_score_tiebreaks "63 16 62",
;;  :loser_sets_won "1",
;;  :loser_games_won "11",
;;  :tourney_year_id "1991-7308",
;;  :tourney_order "1",
;;  :winner_seed "",
;;  :loser_seed "6",
;;  :winner_slug "nicklas-kulti",
;;  :match_order "1",
;;  :loser_name "Michael Stich",
;;  :winner_player_id "k181",
;;  :match_stats_url_suffix "/en/scores/1991/7308/MS001/match-stats",
;;  :tourney_url_suffix "/en/scores/archive/adelaide/7308/1991/results",
;;  :loser_player_id "s351",
;;  :loser_tiebreaks_won "0",
;;  :round_order "1",
;;  :tourney_round_name "Finals",
;;  :match_id "1991-7308-k181-s351",
;;  :winner_name "Nicklas Kulti",
;;  :winner_games_won "13",
;;  :winner_tiebreaks_won "0"}
