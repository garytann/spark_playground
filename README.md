# How to Run
## Requirements
- Java runtime 11 or 17
- Spark-core 2.13
- Spark runtime 4.0.1
- mvn

## Build the JAR file
```
mvn clean
mvn package
```

## Run the spark job sample
- Class Arguemnt: main class of the spark job
- Master Argument: whether to run this job on what mode
- Jar Argument: which JAR file to execute
- topX Argument: what is top ranking to return
- InputParquet1 Argument: file input path for parquet file 1
- InputParquet2 Argument: file input path for partquet 2
- Output Argument: file output path for output.parquet 
```
./bin/spark-submit --class "org.example.Main" --master "local[*]" \
						/Users/htx-ivh/Desktop/spark_playground/target/spark_playground-1.0-SNAPSHOT.jar \
						1 \
						"/Users/htx-ivh/Desktop/spark_playground/inputParquet1.parquet" \ 
						"/Users/htx-ivh/Desktop/spark_playground/inputParquet2.parquet" \ 
						"/Users/htx-ivh/Desktop/output.parquet"
```
