import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.graphframes.GraphFrame;
import org.graphframes.lib.AggregateMessages;

import scala.reflect.ClassTag;

public class FindPath {

    private static final String tmpDirName = "tmp";
    private static final String outputFileNamePattern = "part-00000-*-c000.txt";

    private static final Path tmpDir = new Path(tmpDirName);

    // From:
    // https://stackoverflow.com/questions/3694380/calculating-distance-between-two-points-using-latitude-longitude
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

    private static Dataset<Row> shortestPath(SparkSession spark, GraphFrame g, String start, String end,
                                             String columnName) {
        // Set visited to false and distance to infinity for all vertices except start
        Dataset<Row> vertices = (g.vertices().withColumn("visited", functions.lit(Boolean.FALSE))
                .withColumn("distance", functions.when(g.vertices().col("id").equalTo(start), 0.0)
                        .otherwise(Float.POSITIVE_INFINITY))
                .withColumn("path", functions.array()));
        Dataset<Row> cachedVertices = AggregateMessages.getCachedDataFrame(vertices);
        GraphFrame g2 = new GraphFrame(cachedVertices, g.edges());

        while (!g2.vertices().filter("visited = false").isEmpty()) {
            Object currentNodeId = g2.vertices().filter("visited = false")
                    .sort("distance")
                    .first()
                    .getAs("id");

            // Terminate early upon reaching end
            if (currentNodeId.equals(end)) {
                return (g2.vertices().filter(g2.vertices().col("id").equalTo(end))
                        .withColumn("newPath", functions.array_union(g2.vertices().col("path"),
                                functions.array(g2.vertices().col("id"))))
                        .drop("visited", "path")
                        .withColumnRenamed("newPath", "path"));
            }

            // Sum distance, append path
            Column msgDistance = AggregateMessages.edge().getField(columnName)
                    .plus(AggregateMessages.src().getField("distance"));
            Column msgPath = functions.array_union(AggregateMessages.src().getField("path"),
                    functions.array(AggregateMessages.src().getField("id")));

            // Combine distance and path
            Column msgForDst = functions.when(AggregateMessages.src().getField("id").equalTo(currentNodeId),
                    functions.struct(msgDistance, msgPath));
            // Distance and path from source for nodes that current node can travel to
            Dataset<Row> newDistances = g2.aggregateMessages().sendToDst(msgForDst)
                    .agg(functions.min(AggregateMessages.msg()).alias("aggMess"));

            // Mark current node as visited
            Column newVisitedCol = functions.when(
                    g2.vertices().col("visited")
                            .or((g2.vertices().col("id").equalTo(currentNodeId))),
                    Boolean.TRUE).otherwise(Boolean.FALSE);
            // Update minimum distance for current node
            Column newDistanceCol = functions
                    .when(newDistances.col("aggMess").isNotNull()
                                    .and(newDistances.col("aggMess").getField("col1")
                                            .lt(g2.vertices().col("distance"))),
                            newDistances.col("aggMess").getField("col1"))
                    .otherwise(g2.vertices().col("distance"));
            // Update path with exact same criterion as that for distance
            Column newPathCol = functions.when(
                            newDistances.col("aggMess").isNotNull()
                                    .and(newDistances.col("aggMess").getField("col1")
                                            .lt(g2.vertices().col("distance"))),
                            newDistances.col("aggMess").getField("col2")
                                    .cast("array<string>"))
                    .otherwise(g2.vertices().col("path"));

            // Update vertices with the above fields to store them
            Dataset<Row> newVertices = g2.vertices()
                    .join(newDistances, g2.vertices().col("id").equalTo(newDistances.col("id")), "leftouter")
                    .drop(newDistances.col("id"))
                    .withColumn("visited", newVisitedCol)
                    .withColumn("newDistance", newDistanceCol)
                    .withColumn("newPath", newPathCol)
                    .drop("aggMess", "distance", "path")
                    .withColumnRenamed("newDistance", "distance")
                    .withColumnRenamed("newPath", "path");
            Dataset<Row> cachedNewVertices = AggregateMessages.getCachedDataFrame(newVertices);
            g2 = new GraphFrame(cachedNewVertices, g2.edges());
        }

        // Return empty dataframe, unable to find path to end
        return (spark.createDataFrame(spark.sparkContext().emptyRDD(ClassTag.apply(Row.class)),
                        g.vertices().schema())
                .withColumn("path", functions.array()));

    }

    public static void main(String[] args) throws Exception {
        String outputDir = args[2].split(Path.SEPARATOR)[0];

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
                .withColumn("end", functions.split(input.col("value"), " ").getItem(1));
        List<String> startList = input.select("start")
                .collectAsList()
                .stream()
                .map(r -> r.get(0).toString())
                .collect(Collectors.toList());
        List<String> endList = input.select("end")
                .collectAsList().stream()
                .map(r -> r.get(0).toString())
                .collect(Collectors.toList());

        Dataset<Row> highwayDf = wayDf.where("array_contains(tag._k,'highway')");

        Dataset<Row> v = nodeDf.select(nodeDf.col("_id").cast("string").as("id"), nodeDf.col("_lat"),
                nodeDf.col("_lon"));

        Dataset<Row> pathDf = highwayDf.select("nd._ref")
                .selectExpr("cast(_ref as array<string>) _ref");
        Dataset<Row> revPathDf = highwayDf
                .where("!array_contains(tag,named_struct('_VALUE', CAST(NULL as string), '_k','oneway','_v','yes'))")
                .select("nd._ref").selectExpr("cast(_ref as array<string>) _ref");

        Dataset<Row> srcDf = pathDf.flatMap((FlatMapFunction<Row, String>) n -> {
            List<String> list = ((List<String>) (Object) (n.getList(0)));
            return list.subList(0, list.size() - 1).iterator();
        }, Encoders.STRING()).withColumnRenamed("value", "src");

        Dataset<Row> dstDf = pathDf.flatMap((FlatMapFunction<Row, String>) n -> {
            List<String> list = ((List<String>) (Object) (n.getList(0)));
            return list.subList(1, list.size()).iterator();
        }, Encoders.STRING()).withColumnRenamed("value", "dst");

        Dataset<Row> revSrcDf = revPathDf.flatMap((FlatMapFunction<Row, String>) n -> {
            List<String> list = ((List<String>) (Object) (n.getList(0)));
            return list.subList(1, list.size()).iterator();
        }, Encoders.STRING()).withColumnRenamed("value", "src");

        Dataset<Row> revDstDf = revPathDf.flatMap((FlatMapFunction<Row, String>) n -> {
            List<String> list = ((List<String>) (Object) (n.getList(0)));
            return list.subList(0, list.size() - 1).iterator();
        }, Encoders.STRING()).withColumnRenamed("value", "dst");

        Dataset<Row> fullSrcDf = srcDf.union(revSrcDf);
        Dataset<Row> fullDstDf = dstDf.union(revDstDf);
        fullSrcDf = fullSrcDf.withColumn("id", functions.monotonically_increasing_id());
        fullDstDf = fullDstDf.withColumn("id", functions.monotonically_increasing_id());

        Dataset<Row> e = fullSrcDf
                .join(fullDstDf, "id")
                .select("src", "dst");

        Dataset<Row> deadEnds = e.select("dst")
                .except(e.select("src"));
        e = e.unionByName(deadEnds.select("dst")
                .withColumnRenamed("dst", "src")
                .withColumn("dst", functions.lit(null).cast("string")));

        Dataset<Row> v1 = v.withColumnRenamed("id", "id1")
                .withColumnRenamed("_lat", "lat1")
                .withColumnRenamed("_lon", "lon1");
        Dataset<Row> v2 = v.withColumnRenamed("id", "id2")
                .withColumnRenamed("_lat", "lat2")
                .withColumnRenamed("_lon", "lon2");

        UserDefinedFunction udfDistance = functions.udf(
                (Double lat1, Double lat2, Double lon1, Double lon2) -> distance(
                        Optional.ofNullable(lat1).orElse(0.0),
                        Optional.ofNullable(lat2).orElse(0.0),
                        Optional.ofNullable(lon1).orElse(0.0),
                        Optional.ofNullable(lon2).orElse(0.0)),
                DataTypes.DoubleType);
        spark.udf().register("udfDistance", udfDistance);
        e = e.join(v1, e.col("src").equalTo(v1.col("id1")), "inner")
                .join(v2, e.col("dst").equalTo(v2.col("id2")), "leftouter");

        e = e.withColumn("dist", functions.callUDF("udfDistance", e.col("lat1"), e.col("lat2"), e.col("lon1"),
                        e.col("lon2")))
                .select("src", "dst", "dist");

        GraphFrame g = new GraphFrame(v, e).dropIsolatedVertices();

        Dataset<Row> adjMap = g.edges()
                .distinct()
                .coalesce(1)
                .groupBy("src")
                .agg(functions.collect_list("dst").as("dst"));

        adjMap.withColumn("dst", functions.concat_ws(" ", adjMap.col("dst")))
                .map((MapFunction<Row, String>) x -> x.get(0).toString() + " " + x.get(1).toString(),
                        Encoders.STRING())
                .write()
                .text(tmpDirName);

        FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
        String file = fs.globStatus(new Path(tmpDir + Path.SEPARATOR + outputFileNamePattern))[0].getPath()
                .getName();
        if (!fs.exists(new Path(outputDir))) {
            fs.mkdirs(new Path(outputDir));
        }
        fs.rename(new Path(tmpDir + Path.SEPARATOR + file), new Path(args[2]));

        fs.deleteOnExit(tmpDir);

        FSDataOutputStream fsDataOutputStream = fs.create(new Path(args[3]));
        BufferedWriter bufferedWriter = new BufferedWriter(
                new OutputStreamWriter(fsDataOutputStream));
        for (int i = 0; i < startList.size(); i++) {
            List<String> path = shortestPath(spark, g, startList.get(i), endList.get(i), "dist")
                    .select("path")
                    .first().getList(0);
            bufferedWriter.write(String.join(" -> ", path));
            bufferedWriter.newLine();
            bufferedWriter.flush();
        }

        bufferedWriter.close();
        fs.close();
        spark.stop();
    }
}
