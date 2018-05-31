#/bin/bash
mvn clean package
cd target
rm -rf ../data/output
hadoop jar my-hadoop-1.0.0.jar com.hadoop.app.WordCount ../data/input/ ../data/output
