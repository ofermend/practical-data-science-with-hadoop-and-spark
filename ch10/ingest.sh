# ingest kdd99 dataset
wget http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data.gz
gunzip kddcup.data.gz
hadoop fs -rm -r /user/hdfs/kdd 
hadoop fs -mkdir /user/hdfs/kdd
hadoop fs -put kddcup.data /user/hdfs/kdd/
rm kddcup.data 

