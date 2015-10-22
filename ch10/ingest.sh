# ingest Ohsumed text collection
wget http://disi.unitn.it/moschitti/corpora/ohsumed-all-docs.tar.gz
tar -zxvf ohsumed-all-docs.tar.gz
hadoop fs -rm -r ohsumed
hadoop fs -mkdir ohsumed
hadoop fs -put ohsumed-all/* ohsumed/
rm -rf ohsumed-all
rm ohsumed-all-docs.tar.gz

# copy stop-words to HDFS
hadoop fs -put stop-words.txt .

# get openNLP jar
wget http://apache.mirrors.hoobly.com/opennlp/opennlp-1.6.0/apache-opennlp-1.6.0-bin.zip
unzip apache-opennlp-1.6.0-bin.zip
cp apache-opennlp-1.6.0/lib/opennlp-tools-1.6.0.jar .
rm -rf apache-opennl-1.6.0/
rm apache-opennlp-1.6.0-bin.zip
