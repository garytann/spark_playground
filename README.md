# Location Ranking with Spark
Data Engineering Test

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
```
./bin/spark-submit --class "org.example.Main" --master "local[*]" \
						/Users/htx-ivh/Desktop/spark_playground/target/spark_playground-1.0-SNAPSHOT.jar \
						1 \
						"/Users/htx-ivh/Desktop/spark_playground/inputParquet1.parquet" \ 
						"/Users/htx-ivh/Desktop/spark_playground/inputParquet2.parquet" \ 
						"/Users/htx-ivh/Desktop/output.parquet"
```
