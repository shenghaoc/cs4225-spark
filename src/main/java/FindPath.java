import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.graphframes.GraphFrame;
import org.graphframes.lib.AggregateMessages;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FindPath {
    // From: https://stackoverflow.com/questions/3694380/calculating-distance-between-two-points-using-latitude-longitude
    private static double distance(double lat1, double lat2, double lon1, double lon2) {
        final int R = 6371; // Radius of the earth
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                        * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c * 1000; // convert to meters
        double height = 0; // For this assignment, we assume all locations have the same height.
        distance = Math.pow(distance, 2) + Math.pow(height, 2);
        return Math.sqrt(distance);
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[4]")
                .appName("FindPath")
                .getOrCreate();

        Dataset<Row> nodeDf = spark.read()
                .format("xml")
                .option("rowTag", "node")
                .load(args[0]);

        Dataset<Row> wayDf = spark.read()
                .format("xml")
                .option("rowTag", "way")
                .load(args[0]);

        Dataset<Row> highwayDf = wayDf.where("array_contains(tag._k,'highway')");
        Dataset<Row> revHighwayDf = highwayDf
                .where("!array_contains(tag,named_struct('_VALUE', CAST(NULL as string), '_k','oneway','_v','yes'))");

        Dataset<Row> v = nodeDf.select("_id", "_lat", "_lon").withColumnRenamed("_id", "id");

        Dataset<Row> srcDf = highwayDf.select("nd._ref").flatMap((FlatMapFunction<Row, Long>) n -> {
            List<Long> list = ((List<Long>) (Object) (n.getList(0)));
            return list.subList(0, list.size() - 1).iterator();
        }, Encoders.LONG()).withColumnRenamed("value", "src");

        Dataset<Row> dstDf = highwayDf.select("nd._ref").flatMap((FlatMapFunction<Row, Long>) n -> {
            List<Long> list = ((List<Long>) (Object) (n.getList(0)));
            return list.subList(1, list.size()).iterator();
        }, Encoders.LONG()).withColumnRenamed("value", "dst");

        Dataset<Row> revSrcDf = revHighwayDf.select("nd._ref").flatMap((FlatMapFunction<Row, Long>) n -> {
            List<Long> list = ((List<Long>) (Object) (n.getList(0)));
            return list.subList(1, list.size()).iterator();
        }, Encoders.LONG()).withColumnRenamed("value", "src");

        Dataset<Row> revDstDf = revHighwayDf.select("nd._ref").flatMap((FlatMapFunction<Row, Long>) n -> {
            List<Long> list = ((List<Long>) (Object) (n.getList(0)));
            return list.subList(0, list.size() - 1).iterator();
        }, Encoders.LONG()).withColumnRenamed("value", "dst");

        Dataset<Row> fullSrcDf = srcDf.union(revSrcDf);
        Dataset<Row> fullDstDf = dstDf.union(revDstDf);
        fullSrcDf = fullSrcDf.withColumn("id", functions.monotonically_increasing_id());
        fullDstDf = fullDstDf.withColumn("id", functions.monotonically_increasing_id());

        Dataset<Row> e = fullSrcDf.join(fullDstDf, "id");

        GraphFrame g = new GraphFrame(v, e);

        Dataset<Row> tmpEdges = g.edges().select("src", "dst")
                .coalesce(1).groupBy("src")
                .agg(functions.collect_list("dst").as("dst"));

        tmpEdges.withColumn("dst", functions.concat_ws(" ", tmpEdges.col("dst")))
                .map((MapFunction<Row, String>) x -> x.get(0).toString() + " " + x.get(1).toString(), Encoders.STRING())
                .write()
                .text(args[2]);

        spark.stop();
    }
}
