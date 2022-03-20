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

    private static final String tmpDirName = "tmp";
    private static final String outputFileNamePattern = "part-00000-*-c000.txt";

    private static final org.apache.hadoop.fs.Path tmpDir = new org.apache.hadoop.fs.Path(tmpDirName);

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

    public static void main(String[] args) throws Exception {
        String outputDir = args[2].split(org.apache.hadoop.fs.Path.SEPARATOR)[0];

        SparkSession spark = SparkSession.builder()
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

        Dataset<Row> input = spark.read().text(args[1]);
        input = input.withColumn("start", functions.split(input.col("value"), " ").getItem(0))
                .withColumn("end", functions.split(input.col("value"), " ").getItem(1))
                .drop("value");
        List<String> startList = input.select("start").collectAsList().stream().map(r -> r.get(0).toString()).collect(java.util.stream.Collectors.toList());
        List<String> endList = input.select("end").collectAsList().stream().map(r -> r.get(0).toString()).collect(java.util.stream.Collectors.toList());

        Dataset<Row> highwayDf = wayDf.where("array_contains(tag._k,'highway')");
        Dataset<Row> revHighwayDf = highwayDf
                .where("!array_contains(tag,named_struct('_VALUE', CAST(NULL as string), '_k','oneway','_v','yes'))");

        Dataset<Row> v = nodeDf.select(nodeDf.col("_id").cast("string").as("id"), nodeDf.col("_lat"),
                nodeDf.col("_lon"));

        Dataset<Row> pathDf = highwayDf.select("nd._ref").selectExpr("cast(_ref as array<string>) _ref");
        Dataset<Row> revPathDf = revHighwayDf.select("nd._ref").selectExpr("cast(_ref as array<string>) _ref");

        Dataset<Row> srcDf = pathDf.flatMap((FlatMapFunction<Row, String>) n -> {
            List<String> list = n.getList(0).stream().map(Object::toString)
                    .collect(java.util.stream.Collectors.toList());
            return list.subList(0, list.size() - 1).iterator();
        }, Encoders.STRING()).withColumnRenamed("value", "src");

        Dataset<Row> dstDf = pathDf.flatMap((FlatMapFunction<Row, String>) n -> {
            List<String> list = n.getList(0).stream().map(Object::toString)
                    .collect(java.util.stream.Collectors.toList());
            return list.subList(1, list.size()).iterator();
        }, Encoders.STRING()).withColumnRenamed("value", "dst");

        Dataset<Row> revSrcDf = revPathDf.flatMap((FlatMapFunction<Row, String>) n -> {
            List<String> list = n.getList(0).stream().map(Object::toString)
                    .collect(java.util.stream.Collectors.toList());
            return list.subList(1, list.size()).iterator();
        }, Encoders.STRING()).withColumnRenamed("value", "src");

        Dataset<Row> revDstDf = revPathDf.flatMap((FlatMapFunction<Row, String>) n -> {
            List<String> list = n.getList(0).stream().map(Object::toString)
                    .collect(java.util.stream.Collectors.toList());
            return list.subList(0, list.size() - 1).iterator();
        }, Encoders.STRING()).withColumnRenamed("value", "dst");

        Dataset<Row> fullSrcDf = srcDf.union(revSrcDf);
        Dataset<Row> fullDstDf = dstDf.union(revDstDf);
        fullSrcDf = fullSrcDf.withColumn("id", functions.monotonically_increasing_id());
        fullDstDf = fullDstDf.withColumn("id", functions.monotonically_increasing_id());

        Dataset<Row> e = fullSrcDf
                .join(fullDstDf, "id")
                .select("src", "dst");

        Dataset<Row> deadEnds = e.select("dst").except(e.select("src"));
        e = e.unionByName(deadEnds.select("dst").withColumnRenamed("dst", "src").withColumn("dst",
                functions.lit(null).cast("string")));

        Dataset<Row> v1 = v.withColumnRenamed("id", "id1").withColumnRenamed("_lat", "lat1").withColumnRenamed("_lon",
                "lon1");
        Dataset<Row> v2 = v.withColumnRenamed("id", "id2").withColumnRenamed("_lat", "lat2").withColumnRenamed("_lon",
                "lon2");

        org.apache.spark.sql.expressions.UserDefinedFunction udfDistance = functions.udf(
                (Double lat1, Double lat2, Double lon1, Double lon2) -> distance(java.util.Optional.ofNullable(lat1).orElse(0.0), java.util.Optional.ofNullable(lat2).orElse(0.0), java.util.Optional.ofNullable(lon1).orElse(0.0), java.util.Optional.ofNullable(lon2).orElse(0.0)),
                org.apache.spark.sql.types.DataTypes.DoubleType);
        spark.udf().register("udfDistance", udfDistance);
        e = e.join(v1, e.col("src").equalTo(v1.col("id1")), "inner")
                .join(v2, e.col("dst").equalTo(v2.col("id2")), "leftouter");

        e = e.withColumn("dist",
                functions.callUDF("udfDistance", e.col("lat1"), e.col("lat2"), e.col("lon1"), e.col("lon2")))
                .select("src", "dst", "dist");

        GraphFrame g = new GraphFrame(v, e);

        v.cache();
        e.cache();

        Dataset<Row> tmpEdges = g.edges()
                .distinct()
                .coalesce(1)
                .groupBy("src")
                .agg(functions.collect_list("dst").as("dst"));

        tmpEdges.withColumn("dst", functions.concat_ws(" ", tmpEdges.col("dst")))
                .map((MapFunction<Row, String>) x -> x.get(0).toString() + " " + x.get(1).toString(), Encoders.STRING())
                .write()
                .text(tmpDirName);

        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext().hadoopConfiguration());
        String file = fs.globStatus(new org.apache.hadoop.fs.Path(tmpDir + org.apache.hadoop.fs.Path.SEPARATOR + outputFileNamePattern))[0].getPath().getName();
        if (!fs.exists(new org.apache.hadoop.fs.Path(outputDir))) {
            fs.mkdirs(new org.apache.hadoop.fs.Path(outputDir));
        }
        fs.rename(new org.apache.hadoop.fs.Path(tmpDir + org.apache.hadoop.fs.Path.SEPARATOR + file), new org.apache.hadoop.fs.Path(args[2]));

        fs.deleteOnExit(tmpDir);

        spark.stop();
    }
}
