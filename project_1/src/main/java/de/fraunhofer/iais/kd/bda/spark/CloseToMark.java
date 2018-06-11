package de.fraunhofer.iais.kd.bda.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class CloseToMark {
	private static final double distanceLimit = 0.85;
	private static final String targetArt = "Mark Knopfler";

	public static void main(String[] args) {
		String inputFile = "resources/last-fm-sample1000000.tsv";
		String appName = "CloseToMark";

		SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[*]");

		JavaSparkContext context = new JavaSparkContext(conf);

		// Read file
		JavaRDD<String> input = context.textFile(inputFile);

		JavaPairRDD<String, String> words = input.mapToPair(line -> {
			String[] parts = line.split("\t");
			return new Tuple2<String, String>(parts[3], parts[0]);
		});

		// User set for each artist
		JavaPairRDD<String, UserSet> artUsers = words.aggregateByKey(new UserSet(), (agg, value) -> agg.add(value),
				(agg1, agg2) -> agg1.add(agg2));
		// Mark's user set
		UserSet targetArtUser = artUsers.collectAsMap().get(targetArt);

		// Calculate each artists distance to Mark and filter by the limit 0.85
		JavaPairRDD<String, Double> artDistances = artUsers.mapToPair((artUser) -> {
			return new Tuple2<String, Double>(artUser._1, artUser._2.distanceTo(targetArtUser));
		}).filter((artDistance) -> (artDistance._2 < distanceLimit));

		artDistances.saveAsTextFile("/tmp/closetomark.txt");
		context.close();

	}

}
