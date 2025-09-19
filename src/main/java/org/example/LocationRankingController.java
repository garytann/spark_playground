package org.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class LocationRankingController {
    public static JavaRDD<LocationRankingModel> getLocationRanking(JavaRDD<Row> df, JavaRDD<Row> df2, int topX) {

        // Lookup the geographical id and item name tuple and count how many occurrences
        JavaPairRDD<Tuple2<Long, String>, Integer> itemCounts = df
                .mapToPair(row -> new Tuple2<>(
                        new Tuple2<>(
                                row.getLong(row.fieldIndex("geographical_location_oid")),
                                row.getString(row.fieldIndex("item_name"))
                        ),1
                ))
                .reduceByKey(Integer::sum);

//        itemCounts.foreach(tuple -> System.out.println(tuple));

        // Transform the tuple to be key: geographical_location_oid, value: (item_name, count) and group all them by key
        JavaPairRDD<Long, Iterable<Tuple2<String, Integer>>> groupedByLocation = itemCounts
                .mapToPair(tuple ->
                        new Tuple2<>(tuple._1._1, new Tuple2<>(tuple._1._2, tuple._2)))
                .groupByKey();

//        groupedByLocation.foreach(tuple -> System.out.println(tuple));


        // Rank the items by count for each geographical location id
        // Format the rankItems as a key-value pair with the value being a list of tuples (item_name, count)
        JavaPairRDD<Long, List<Tuple2<String, Integer>>> rankedItems = groupedByLocation.mapValues(iterable -> {
            List<Tuple2<String, Integer>> sortedList = new ArrayList<>();
            iterable.forEach(sortedList::add);
            // Sort by count descending
            sortedList.sort((a, b) -> b._2.compareTo(a._2));
            return sortedList;
        });

//        rankedItems.foreach(tuple -> System.out.println(tuple));

        // Create a key-value pair of the location map
        JavaPairRDD<Long, String> locationMap = df2
                .mapToPair(row -> new Tuple2<>(
                        row.getLong(row.fieldIndex("geographical_location_oid")),
                        row.getString(row.fieldIndex("geographical_location"))));


        // Get the top X items for each geographical location id and slice the list to only keep top X
        JavaPairRDD<Long, List<Tuple2<String, Integer>>> topRankedItems = rankedItems.mapValues(list ->
                list.subList(0, Math.min(topX, list.size()))
        );

        // Join the ranked items with the location map
        JavaPairRDD<Long, Tuple2<List<Tuple2<String, Integer>>, String>> joinedDF = topRankedItems.join(locationMap);

        // 1. Map the JavaRDD<Row> to JavaRDD<LocationRankingModel>
        // 2. For each location, iterate through the list of (item_name, count) tuples and create a LocationRankingModel
        // 3. Set the geographical location, item name, and rank in the LocationRankingModel
        // 4. Return the JavaRDD<LocationRankingModel>
        JavaRDD<LocationRankingModel> locationRankingRDD = joinedDF.flatMap(tuple -> {
            String location = tuple._2._2;
            List<Tuple2<String, Integer>> items = tuple._2._1;
            List<LocationRankingModel> models = new ArrayList<>();
            int rankCount = 1;
            for (Tuple2<String, Integer> item : items) {
                LocationRankingModel model = new LocationRankingModel();
                // This allows for more aggregation in the future if needed by
                // using the model to add in the column as required
                model.setGeographicalLocation(location);
                model.setItemName(item._1);
                model.setRank(rankCount);
                models.add(model);
                rankCount++;
            }
            return models.iterator();
        });
        return locationRankingRDD;
    }
}
