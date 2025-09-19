import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.DatasetGenerator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RowCountTest {
    protected static SparkSession spark;

    @BeforeAll
    public static void setUp() {
        System.setProperty("spark.testing", "true"); // special flag for test mode
        spark = SparkSession.builder()
                .appName("Spark Unit Test")
                .master("local[*]")
                .getOrCreate();
    }

    @AfterAll
    public static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    public void testWordCountOnMockData() {
        Dataset<Row> df = DatasetGenerator.generateMockData2(spark);
        JavaRDD<Row> javaDF = df.javaRDD();

        JavaPairRDD<Long, String> locationMap = javaDF
                .mapToPair(row -> new Tuple2<>(
                        row.getLong(row.fieldIndex("geographical_location_oid")),
                        row.getString(row.fieldIndex("geographical_location"))));
        assertEquals(5, locationMap.count());

    }
}