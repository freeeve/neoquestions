(ns neoquestions.core
  (:use cheshire.core)
  (:require [clojure.java.jdbc :as sql]
            [clj-http.client :as client]
            [oauth.client :as oauth]
            [twitter :as twitter]
            [clojure.java.io :as io])
  (:import [java.io PushbackReader])
  (:import [org.apache.commons.lang3 StringEscapeUtils]))

(def dbspec {:classname "org.sqlite.JDBC"
             :subprotocol "sqlite"
             :subname "tweets.sqlite"
             :db "tweets.sqlite"})

(defn create-tables
  "one time table creation."
  []
  (do
    (sql/create-table
      :questions
      [:question_id :integer "PRIMARY KEY"]
      [:title "varchar(128)"]
      [:link "varchar(200)"]
      [:tweeted :integer])))

(defn invoke-with-connection [f]
  (sql/with-connection
    dbspec
    (sql/transaction
      (f))))

(defn insert-question 
  [q]
  ;(println (str "inserting" q))
  (try
    (invoke-with-connection 
      (fn [] (sql/insert-record "questions" 
                                {:question_id (get q "question_id")
                                 :title (get q "title")
                                 :link (get q "link")
                                 :tweeted 0}))) 
  (catch Exception e ())))

(def conf (with-open [r (io/reader "settings.clj")]
   (read (PushbackReader. r))))

(def oauth-consumer (oauth/make-consumer (:consumer_key conf)
                                         (:consumer_secret conf) 
                                         "https://api.twitter.com/oauth/request_token"
                                         "https://api.twitter.com/oauth/access_token"
                                         "https://api.twitter.com/oauth/authorize"
                                         :hmac-sha1))

(defn fetch-latest-questions
  []
  (let [a-day-ago (- (int (/ (System/currentTimeMillis) 1000)) (* 60 60 24))
        response (client/get (str "https://api.stackexchange.com/2.1/search?fromdate=" a-day-ago "&order=asc&sort=creation&tagged=neo4j&site=stackoverflow"))
        questions (get (parse-string (:body response)) "items")]
    (doseq [q questions] (insert-question q))))

(defn tweet-question
  [question]
  (let [title (StringEscapeUtils/unescapeHtml4 (:title question))
        tweet (str 
                \"
                (if (> (count title) 100)
                    (str (subs title 0 100) "...")
                    title)
                "\" #neo4j " 
                (:link question))]
    (println tweet)
    (twitter/with-oauth oauth-consumer 
                        (:access_token conf)
                        (:access_token_secret conf)
                        (twitter/update-status tweet)))
  (invoke-with-connection
    (fn [] (sql/update-values "questions" ["question_id=?" (:question_id question)] {:tweeted 1}))))

(defn tweet-questions
  []
  (let [rs (invoke-with-connection
             (fn [] (sql/with-query-results rs 
                                            ["select * from questions where tweeted=0"] rs)))]
    (doseq [row rs] (tweet-question row))))

(defn -main
  [& args]
  (while true
    (try
      (fetch-latest-questions)
      (tweet-questions)
      (catch Exception e (println "Exception: " e)))
    (Thread/sleep (* 1000 600)))
)
