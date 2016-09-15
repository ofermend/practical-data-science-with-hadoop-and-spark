DROP TABLE tweets_raw;
CREATE EXTERNAL TABLE tweets_raw (
  `polarity` int,
  `id` string,
  `date` string,
  `query` string,
  `user` string,
  `text`  string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/user/jupyter/sentiment'
tblproperties("skip.header.line.count"="0");

DROP TABLE tweets;
CREATE TABLE tweets STORED AS ORC AS SELECT * FROM tweets_raw;

DROP TABLE sentiment_words;
CREATE EXTERNAL TABLE sentiment_words (
  `word` string,
  `score` int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/jupyter/wordlist/';

