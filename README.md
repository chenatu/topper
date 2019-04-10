# Algorithm
1. hash file into splits
2. count url count of each split
3. merge sort of each word count file

# Tech stack
jdk 1.8 + maven 3

# How to run
## Compile and build
`mvn package`

## generate file
`java -cp topper-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.chenatu.topper.GenFile 100000000000 url.dat`

## count the top k url
`nohup java -XX:+UseG1GC -Xmx1g -cp topper-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.chenatu.topper.TopK url.dat splits 1000 4 2 100 > split.log 2>&1 &`

# Benchmark
get top 100 url of 100GB file in such vm:

cpu model: Intel(R) Xeon(R) CPU E5-2698 v4 @ 2.20GHz

cpu cores: 4

disk: hdd with read 350MB/sec and seq write 350MB/sec, IOPS 85k/s

With the algorithm above the total time cost is 8855s

