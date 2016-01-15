To build and run

```
mvn package

set RAW_PATH=C:\Users\Mathias\vagrant\hadoop\document-analysis\input\03_A_ranger
set RAW_PATH=C:\Users\Mathias\vagrant\hadoop\document-analysis\input\test
set AVRO_PATH=C:\Users\Mathias\vagrant\hadoop\document-analysis\output\avro
set PARSED_PATH=C:\Users\Mathias\vagrant\hadoop\document-analysis\output\parsed
set TESSERACT_PATH=C:\Users\Mathias\Work\Tools\tessdata-master
set TESSERACT_LANG=fra

java -cp target\scala-2.10\document-analysis-1.0-SNAPSHOT-hadoop2.6.0.jar org.grozeille.CopyToAvro -i %RAW_PATH% -o %AVRO_PATH%

set SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005



spark-submit --master local[8] --class org.grozeille.DocumentAnalysisAvro target\scala-2.10\document-analysis-1.0-SNAPSHOT-hadoop2.6.0.jar -i %AVRO_PATH% -o %PARSED_PATH% -t %TESSERACT_PATH%
```