package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class DatasetGenerator {
    public static Dataset<Row> generateMockData(SparkSession spark) {

        List<Row> data = Arrays.asList(
                RowFactory.create(1L, 101L, 1001L, "bicycle", 1718000000000L),
                RowFactory.create(1L, 101L, 1001L, "bicycle", 1718000000000L),
                RowFactory.create(1L, 102L, 1002L, "car", 1718000001000L),
                RowFactory.create(2L, 201L, 2001L, "dog", 1718000002000L),
                RowFactory.create(2L, 202L, 2002L, "bicycle", 1718000003000L),
                RowFactory.create(3L, 301L, 3001L, "person", 1718000004000L),
                RowFactory.create(1L, 103L, 1003L, "bus", 1718000005000L),
                RowFactory.create(1L, 104L, 1004L, "truck", 1718000006000L),
                RowFactory.create(2L, 203L, 2003L, "cat", 1718000007000L),
                RowFactory.create(2L, 204L, 2004L, "motorcycle", 1718000008000L),
                RowFactory.create(3L, 302L, 3002L, "bicycle", 1718000009000L),
                RowFactory.create(3L, 303L, 3003L, "car", 1718000010000L),
                RowFactory.create(1L, 105L, 1005L, "dog", 1718000011000L),
                RowFactory.create(1L, 106L, 1006L, "person", 1718000012000L),
                RowFactory.create(2L, 205L, 2005L, "bus", 1718000013000L),
                RowFactory.create(2L, 206L, 2006L, "truck", 1718000014000L),
                RowFactory.create(3L, 304L, 3004L, "cat", 1718000015000L),
                RowFactory.create(3L, 305L, 3005L, "motorcycle", 1718000016000L),
                RowFactory.create(1L, 107L, 1007L, "bicycle", 1718000017000L),
                RowFactory.create(1L, 108L, 1008L, "car", 1718000018000L),
                RowFactory.create(2L, 207L, 2007L, "dog", 1718000019000L),
                RowFactory.create(2L, 208L, 2008L, "person", 1718000020000L),
                RowFactory.create(3L, 306L, 3006L, "bus", 1718000021000L),
                RowFactory.create(3L, 307L, 3007L, "truck", 1718000022000L),
                RowFactory.create(1L, 109L, 1009L, "cat", 1718000023000L),
                RowFactory.create(1L, 110L, 1010L, "motorcycle", 1718000024000L),
                RowFactory.create(2L, 209L, 2009L, "bicycle", 1718000025000L),
                RowFactory.create(2L, 210L, 2010L, "car", 1718000026000L),
                RowFactory.create(3L, 308L, 3008L, "dog", 1718000027000L),
                RowFactory.create(3L, 309L, 3009L, "person", 1718000028000L),
                RowFactory.create(1L, 111L, 1011L, "bus", 1718000029000L),
                RowFactory.create(1L, 112L, 1012L, "truck", 1718000030000L),
                RowFactory.create(2L, 211L, 2011L, "cat", 1718000031000L),
                RowFactory.create(2L, 212L, 2012L, "motorcycle", 1718000032000L),
                RowFactory.create(3L, 310L, 3010L, "bicycle", 1718000033000L),
                RowFactory.create(3L, 311L, 3011L, "car", 1718000034000L),
                RowFactory.create(1L, 113L, 1013L, "dog", 1718000035000L),
                RowFactory.create(1L, 114L, 1014L, "person", 1718000036000L),
                RowFactory.create(2L, 213L, 2013L, "bus", 1718000037000L),
                RowFactory.create(2L, 214L, 2014L, "truck", 1718000038000L),
                RowFactory.create(3L, 312L, 3012L, "cat", 1718000039000L),
                RowFactory.create(3L, 313L, 3013L, "motorcycle", 1718000040000L),
                RowFactory.create(1L, 115L, 1015L, "bicycle", 1718000041000L),
                RowFactory.create(1L, 116L, 1016L, "car", 1718000042000L),
                RowFactory.create(2L, 215L, 2015L, "dog", 1718000043000L),
                RowFactory.create(2L, 216L, 2016L, "person", 1718000044000L),
                RowFactory.create(3L, 314L, 3014L, "bus", 1718000045000L),
                RowFactory.create(3L, 315L, 3015L, "truck", 1718000046000L),
                RowFactory.create(1L, 117L, 1017L, "cat", 1718000047000L),
                RowFactory.create(1L, 118L, 1018L, "motorcycle", 1718000048000L),
                RowFactory.create(2L, 217L, 2017L, "bicycle", 1718000049000L),
                RowFactory.create(2L, 218L, 2018L, "car", 1718000050000L),
                RowFactory.create(3L, 316L, 3016L, "dog", 1718000051000L),
                RowFactory.create(3L, 317L, 3017L, "person", 1718000052000L)
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("geographical_location_oid", DataTypes.LongType, false, Metadata.empty()),
                new StructField("video_camera_oid", DataTypes.LongType, false, Metadata.empty()),
                new StructField("detection_oid", DataTypes.LongType, false, Metadata.empty()),
                new StructField("item_name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("timestamp_detected", DataTypes.LongType, false, Metadata.empty())
        });

        return spark.createDataFrame(data, schema);
    }

    public static Dataset<Row> generateMockData2(SparkSession spark) {
        List<Row> data = Arrays.asList(
                RowFactory.create(1L, "New York"),
                RowFactory.create(2L, "San Francisco"),
                RowFactory.create(3L, "London"),
                RowFactory.create(4L, "Tokyo"),
                RowFactory.create(5L, "Berlin")
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("geographical_location_oid", DataTypes.LongType, false, Metadata.empty()),
                new StructField("geographical_location", DataTypes.StringType, false, Metadata.empty())
        });

        return spark.createDataFrame(data, schema);
    }
}
