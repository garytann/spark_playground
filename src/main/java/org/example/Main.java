package org.example;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.*;
import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;

import java.util.*;

import org.apache.spark.sql.SparkSession;

import javax.xml.stream.Location;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        // ## 1. Init Dataset 1 and 2
        System.out.println("Init Spark Session");
        SparkSession spark = SparkSession
                .builder()
                .appName("Location Ranking")
                .getOrCreate();
        System.out.println("Init Mock Data");

        // Generating a mock dataset for own testing
//        Dataset<Row> parquetFileDF = DatasetGenerator.generateMockData(spark);
//        Dataset<Row> parquetFileDF2 = DatasetGenerator.generateMockData2(spark);

        // Reading from existing parquet files
        String parquetFilePath1 = "src/main/parquet/mock_data1.parquet";
        String parquetFilePath2 = "src/main/parquet/mock_data2.parquet";

        Dataset<Row> parquetFileDF = spark.read().parquet(parquetFilePath1);
        Dataset<Row> parquetFileDF2 = spark.read().parquet(parquetFilePath2);


//        ## 1.1 Display dataframes
        parquetFileDF.show();
        parquetFileDF2.show();

        // ## 2. Init RDD Spark session
//        System.out.println("Init Spark Context");
//        SparkConf conf = new SparkConf().setAppName("TopX Location").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);


        //## 3. Parallelize Dataframes with RDD
        JavaRDD<Row> df = parquetFileDF.javaRDD();
        JavaRDD<Row> df2 = parquetFileDF2.javaRDD();

        // Transforation: ensure all columns are distinct
        JavaRDD<Row> distinctDF = df.distinct();

        JavaRDD<LocationRankingModel> locationRankingRDD = LocationRankingController.getLocationRanking(distinctDF, df2, 3);

        // Define the encoder for LocationRankingModel
        Encoder<LocationRankingModel> rankingEncoder = Encoders.bean(LocationRankingModel.class);

        // Convert the JavaRDD<LocationRankingModel> to a Dataset
        Dataset<LocationRankingModel> javaBeanDS = spark.createDataset(locationRankingRDD.collect(), rankingEncoder);

        // Show the Dataset
        javaBeanDS.show();


        spark.stop();


    }
}