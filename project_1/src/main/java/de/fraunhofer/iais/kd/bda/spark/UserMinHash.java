package de.fraunhofer.iais.kd.bda.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class UserMinHash {
	/**
	 * (Mark Knopfler, 22, 50, 10, 58, 57, 14, 15, 10, 27, 8, 10, 4, 74, 68, 1, 9, 1, 3, 5, 36)
     * (Mark Knopfler & Eric Clapton, 298, 921, 141, 372, 830, 50, 502, 731, 174, 853, 83, 762, 209, 436, 896, 564, 236, 467, 137, 593)
     * (Mark Knopfler & Chet Atkins, 514, 236, 681, 119, 0, 445, 320, 765, 640, 958, 404, 722, 601, 35, 927, 234, 554, 1, 310, 189)
     * (Mark Knopfler & Emmylou Harris, 18, 50, 34, 57, 44, 248, 140, 89, 148, 65, 25, 106, 77, 0, 31, 9, 37, 8, 149, 97)
	 * 
	 */
	public static void main(String[] args) {
		
		String inputFile= "resources/last-fm-sample1000000.tsv";
		String appName = "UserSet";
	
		SparkConf conf  = new SparkConf().setAppName(appName)
										 .setMaster("local[*]");
		
		JavaSparkContext context = new JavaSparkContext(conf);
		
		//Read file
		JavaRDD<String> input = context.textFile(inputFile);
		
		
		JavaPairRDD<String, String> words = input.mapToPair(line-> {
			String[] parts = line.split("\t");
			return new Tuple2<String,String>(parts[3], parts[0]);
		});
		
		JavaPairRDD<String, UserSet> articCount = words.aggregateByKey(
				new UserSet(), (agg, value) -> agg.add(value), (agg1,agg2) -> agg1.add(agg2)
		);

		JavaPairRDD<String, String> minHashes = articCount.mapToPair((tuple) -> {
			String minHash = " " + tuple._2.toMinHashSignature();
			return new Tuple2<String, String>(tuple._1, minHash);
		});
		
		minHashes.saveAsTextFile("/tmp/minhash.txt");
		context.close();
	}
}
