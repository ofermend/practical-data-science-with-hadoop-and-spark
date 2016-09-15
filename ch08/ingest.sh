# ingest sentiment140 dataset
curl http://cs.stanford.edu/people/alecmgo/trainingandtestdata.zip > sentiment.zip
unzip sentiment.zip
hadoop fs -rm sentiment
hadoop fs -mkdir sentiment
hadoop fs -put testdata.manual.2009.06.14.csv training.1600000.processed.noemoticon.csv sentiment/

# ingest positive/negative word list
hadoop fs -rm wordlist
hadoop fs -mkdir wordlist
hadoop fs -put AFINN-111.txt wordlist/

# run hive query
hive -f build-tables.hql
