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

        // Default value
        int topX = 3;
        String parquetFilePath1 = "src/main/parquet/mock_data1.parquet";
        String parquetFilePath2 = "src/main/parquet/mock_data2.parquet";
        String outputFilePath = "src/main/parquet/output_location_ranking.parquet";

        // Reading from arg
        if (args.length > 0) {
            try {
                topX = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid number format for topX. Using default value of 3.");
            }
        }
        if (args.length > 1) {
            try{
                parquetFilePath1 = args[1];

            } catch (Exception e){
                System.err.println("Invalid path for parquetFilePath1. Using default path.");
            }
        }
        if (args.length > 2) {
            try{
                parquetFilePath2 = args[2];
            } catch (Exception e){
                System.err.println("Invalid path for parquetFilePath2. Using default path.");
            }
        }

        if (args.length > 3){
            try{
                outputFilePath = args[3];
            } catch (Exception e){
                System.err.println("Invalid path for outputFilePath. Using default path.");
            }
        }

        // Generating a mock dataset for own testing
//        Dataset<Row> parquetFileDF = DatasetGenerator.generateMockData(spark);
//        Dataset<Row> parquetFileDF2 = DatasetGenerator.generateMockData2(spark);



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

        JavaRDD<LocationRankingModel> locationRankingRDD = LocationRankingController.getLocationRanking(distinctDF, df2, topX);

        // Define the encoder for LocationRankingModel
        Encoder<LocationRankingModel> rankingEncoder = Encoders.bean(LocationRankingModel.class);

        // Convert the JavaRDD<LocationRankingModel> to a Dataset
        Dataset<LocationRankingModel> javaBeanDS = spark.createDataset(locationRankingRDD.collect(), rankingEncoder);

        // Show the Dataset
        javaBeanDS.show();

        // Save as Parquet (overwrite if exists)
        javaBeanDS.write()
                .mode(SaveMode.Overwrite)
                .parquet(outputFilePath);


        spark.stop();


    }
}