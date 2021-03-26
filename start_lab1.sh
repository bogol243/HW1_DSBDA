hdfs dfs -put input/
yarn jar target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar input output
[ -d output ] && rm -r output
hdfs dfs -get output
hadoop fs -text output/part-r-00000 > output/uncompressed_res
