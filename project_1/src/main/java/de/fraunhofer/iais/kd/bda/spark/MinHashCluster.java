package de.fraunhofer.iais.kd.bda.spark;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class MinHashCluster implements Serializable{

	public static void main(String[] args) {

		//String inputFile = "resources/sorted_sample.tsv";
		String inputFile = "resources/last-fm-sample1000000.tsv";
		String appName = "MinHashAggregator";

		SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[*]");

		JavaSparkContext context = new JavaSparkContext(conf);

		// Read file
		JavaRDD<String> input = context.textFile(inputFile);

		JavaPairRDD<String, String> arts = input.mapToPair(line -> {
			String[] parts = line.split("\t");
			return new Tuple2<String, String>(parts[3], parts[0]);
		});

		// aggregate users' hashes
		JavaPairRDD<String, MinHashAggregator> artminhashs = arts.aggregateByKey(new MinHashAggregator(),
				(agg, value) -> agg.add(value), (agg1, agg2) -> agg1.combine(agg2));

		// Initialization of the model
		MinHashClusterModel model = new MinHashClusterModel("resources/sample_centers_users.csv");
		
		//Maps to Artist - Cluster relation
		JavaPairRDD<String, String> artNameClusterMap = artminhashs.mapToPair(tuple -> {
			String key = model.findClosestCluster(tuple._2);
			return new Tuple2<String, String>(tuple._1, key);
		});
		
		artNameClusterMap.saveAsTextFile("/tmp/minhashartcluster.txt");
		
		// Inverts Artist - CLuster relation and gathers all artists, related to one cluster.
		JavaPairRDD<String, String> clusterArtNameMap = artNameClusterMap.mapToPair(tuple -> {
			return new Tuple2<String, String>(tuple._2, tuple._1);
		}).reduceByKey((arg1, arg2) -> arg1 + "," + arg2);
		
		clusterArtNameMap.saveAsTextFile("/tmp/minhashclusterarts.txt");
		context.close();

	}
}
