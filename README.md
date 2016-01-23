To build and run

```
mvn package

set RAW_PATH=C:\Users\Mathias\vagrant\hadoop\document-analysis\input\03_A_ranger
set RAW_PATH=C:\Users\Mathias\vagrant\hadoop\document-analysis\input\test
set AVRO_PATH=C:\Users\Mathias\vagrant\hadoop\document-analysis\output\avro
set PARSED_PATH=C:\Users\Mathias\vagrant\hadoop\document-analysis\output\parsed
set PARSED_PATH=C:\Users\Mathias\vagrant\hadoop\document-analysis\output\parsed_simple
set TESSERACT_PATH=C:\Users\Mathias\Work\Tools\tessdata-master
set TESSERACT_LANG=fra

java -cp target\scala-2.10\document-analysis-1.0-SNAPSHOT-hadoop2.6.0.jar org.grozeille.CopyToAvro -i %RAW_PATH% -o %AVRO_PATH%

set SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005



spark-submit --master local[8] --class org.grozeille.DocumentAnalysisAvro target\scala-2.10\document-analysis-1.0-SNAPSHOT-hadoop2.6.0.jar -i %AVRO_PATH% -o %PARSED_PATH% -t %TESSERACT_PATH%


bin\solr start -c -m 1g -z localhost:2181 -f
bin\solr create -p 8983 -c ineodoc -d basic_configs -s 1 -rf 1
bin\solr delete -p 8983 -c ineodoc

server\scripts\cloud-scripts\zkcli.bat -zkhost localhost:2181 -cmd putfile server/solr/configsets/ineodoc_configs/conf/schema.xml /configs/ineodoc/schema.xml
server\scripts\cloud-scripts\zkcli.bat -zkhost localhost:2181 -cmd upconfig  -confname ineodoc  -confdir server/solr/configsets/ineodoc_configs/conf
curl -X GET "http://localhost:8983/solr/admin/cores?wt=json&action=RELOAD&core=ineodoc_shard1_replica1"

curl -GET "http://localhost:8983/solr/ineodoc/update?stream.body=<delete><query>*:*</query></delete>"


curl -X POST http://localhost:8983/solr/ineodoc/config -d '{"set-property":{"updateHandler.autoSoftCommit.maxTime":"2000"}}'




spark-submit --master local[8] --class org.grozeille.IndexToSolr target\scala-2.10\document-analysis-1.0-SNAPSHOT-hadoop2.6.0.jar -i %PARSED_PATH% -z localhost:2181 -c ineodoc
```