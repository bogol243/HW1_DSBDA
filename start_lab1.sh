# clear the hdfs workspace
hdfs dfs -rm -r output
hdfs dfs -rm -r input
#clear the local workspace
[ -d output ] && rm -r output
[ -d input ] && rm -r input
mkdir input
#generate input files
NUM_LINES=100000
NUM_BROKEN=20
DATE_BEGIN="2020-10-10"
DATE_END="2020-10-15"
NUM_FILES=4
./get_data.py $NUM_LINES $NUM_FILES $NUM_BROKEN $DATE_BEGIN $DATE_END
#upload input data to hdfs
hdfs dfs -put input/
#start the yarn job
yarn jar target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar input output
#download the raw output date to local directory
hdfs dfs -get output
#unpack and download snappy-compressed sequence file
hadoop fs -text output/part-r-00000 > output/uncompressed_res
