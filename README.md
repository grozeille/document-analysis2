To build and run

```
mvn package

set RAW_PATH=C:\Users\Mathias\vagrant\hadoop\document-analysis\input\03_A_ranger
set RAW_PATH=C:\Users\Mathias\vagrant\hadoop\document-analysis\input\test
set RAW_PATH=Z:\AMINA\03_A_ranger
set AVRO_PATH=C:\Users\Mathias\vagrant\hadoop\document-analysis\output\avro
set PARSED_PATH=C:\Users\Mathias\vagrant\hadoop\document-analysis\output\parsed
set PARSED_PATH=C:\Users\Mathias\vagrant\hadoop\document-analysis\output\parsed_simple
set PARSED_PATH=C:\Users\Mathias\vagrant\hadoop\document-analysis\output\parsed_simple2
set PARSED_PATH=C:\Users\Mathias\vagrant\hadoop\document-analysis\output\parsed_simple3
set WORDCOUNT_PATH=C:\Users\Mathias\vagrant\hadoop\document-analysis\output\wordcount
set CLUSTERING_PATH=C:\Users\Mathias\vagrant\hadoop\document-analysis\output\clustering
set CLUSTERING_MODEL_PATH=C:\Users\Mathias\vagrant\hadoop\document-analysis\output\
set LANG_PATH=C:\Users\Mathias\vagrant\hadoop\document-analysis\output\lang
set TESSERACT_PATH=C:\Users\Mathias\Work\Tools\tessdata-master
set TESSERACT_LANG=fra

java -cp target\scala-2.10\document-analysis-1.0-SNAPSHOT-hadoop2.6.0.jar org.grozeille.CopyToAvro -i %RAW_PATH% -o %AVRO_PATH%

set SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005



spark-submit --master local[8] --class org.grozeille.DocumentAnalysisAvro target\scala-2.10\document-analysis-1.0-SNAPSHOT-hadoop2.6.0.jar -i %AVRO_PATH% -o %PARSED_PATH% -t %TESSERACT_PATH% > out 2>&1


bin\solr start -c -m 1g -z localhost:2181 -f
bin\solr create -p 8983 -c ineodoc -d basic_configs -s 1 -rf 1
bin\solr delete -p 8983 -c ineodoc

server\scripts\cloud-scripts\zkcli.bat -zkhost localhost:2181 -cmd putfile server/solr/configsets/ineodoc_configs/conf/schema.xml /configs/ineodoc/schema.xml
server\scripts\cloud-scripts\zkcli.bat -zkhost localhost:2181 -cmd upconfig  -confname ineodoc  -confdir server/solr/configsets/ineodoc_configs/conf
curl -X GET "http://localhost:8983/solr/admin/cores?wt=json&action=RELOAD&core=ineodoc_shard1_replica1"

curl -GET "http://localhost:8983/solr/ineodoc/update?stream.body=<delete><query>*:*</query></delete>"


curl -X POST http://localhost:8983/solr/ineodoc/config -d '{"set-property":{"updateHandler.autoSoftCommit.maxTime":"2000"}}'



spark-submit --master local[8] --class org.grozeille.DetectLang target\scala-2.10\document-analysis-1.0-SNAPSHOT-hadoop2.6.0.jar -i %PARSED_PATH% -o %LANG_PATH%

spark-submit --master local[8] --class org.grozeille.Clustering target\scala-2.10\document-analysis-1.0-SNAPSHOT-hadoop2.6.0.jar -i %PARSED_PATH% -o %CLUSTERING_PATH% -m %CLUSTERING_MODEL_PATH%

spark-submit --master local[8] --class org.grozeille.IndexToSolr target\scala-2.10\document-analysis-1.0-SNAPSHOT-hadoop2.6.0.jar -i %PARSED_PATH% -z localhost:2181 -c ineodoc -l %LANG_PATH% -k %CLUSTERING_PATH%

```

```SQL

DROP TABLE documents;
CREATE EXTERNAL TABLE documents (
  path string,
  body string,
  fileName string,
  lang string)
STORED AS AVRO
LOCATION 'C:\\Users\\Mathias\\vagrant\\hadoop\\document-analysis\\output\\parsed_simple2'
TBLPROPERTIES (
    'avro.schema.literal'='{
                             "type" : "record",
                             "name" : "topLevelRecord",
                             "fields" : [ {
                               "name" : "path",
                               "type" : [ "string", "null" ]
                             }, {
                               "name" : "body",
                               "type" : [ "string", "null" ]
                             }, {
                               "name" : "fileName",
                               "type" : [ "string", "null" ]
                             }, {
                               "name" : "lang",
                               "type" : [ "string", "null" ]
                             } ]
                           }')
;

DROP TABLE lang;
CREATE EXTERNAL TABLE lang (
  path string,
  lang string)
STORED AS AVRO
LOCATION 'C:\\Users\\Mathias\\vagrant\\hadoop\\document-analysis\\output\\lang'
TBLPROPERTIES (
    'avro.schema.literal'='{
                              "type" : "record",
                              "name" : "topLevelRecord",
                              "fields" : [ {
                                "name" : "path",
                                "type" : [ "string", "null" ]
                              }, {
                                "name" : "lang",
                                "type" : [ "string", "null" ]
                              } ]
                            }')
;

DROP TABLE wordcount;
CREATE EXTERNAL TABLE wordcount (
  f string,
  w string,
  c bigint)
STORED AS AVRO
LOCATION 'C:\\Users\\Mathias\\vagrant\\hadoop\\document-analysis\\output\\wordcount'
TBLPROPERTIES (
    'avro.schema.literal'='{
                             "type" : "record",
                             "name" : "topLevelRecord",
                             "fields" : [ {
                               "name" : "f",
                               "type" : [ "string", "null" ]
                             }, {
                               "name" : "w",
                               "type" : [ "string", "null" ]
                             }, {
                               "name" : "c",
                               "type" : [ "long", "null" ]
                             } ]
                           }')
;

DROP TABLE clusters;
CREATE EXTERNAL TABLE clusters (
  path string,
  cluster bigint)
STORED AS AVRO
LOCATION 'C:\\Users\\Mathias\\vagrant\\hadoop\\document-analysis\\output\\clustering'
TBLPROPERTIES (
    'avro.schema.literal'='{
                             "type" : "record",
                             "name" : "topLevelRecord",
                             "fields" : [ {
                               "name" : "path",
                               "type" : [ "string", "null" ]
                             }, {
                               "name" : "cluster",
                               "type" : [ "int", "null" ]
                             } ]
                           }')
;

```