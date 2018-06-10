package de.fraunhofer.iais.kd.bda.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class UserClicks {
	/**
	 * Answer to the question from the Exercise: 
	 * "How many listening events has the artist “Mark Knopfler”"
	 * 
	 * Mark Knopfler - 117
	 * Mark Knopfler & Eric Clapton - 1
	 * Mark Knopfler & Chet Atkins - 1
	 * Mark Knopfler & Emmylou Harris - 10
	 * 
	 * In total: 129
	 */
	
	public static void main(String[] args) {
			
			String inputFile= "resources/last-fm-sample1000000.tsv";
			String appName = "UserClicks";
		
			SparkConf conf  = new SparkConf().setAppName(appName)
											 .setMaster("local[*]");
			
			JavaSparkContext context = new JavaSparkContext(conf);
			
			//Read file
			JavaRDD<String> input = context.textFile(inputFile);
			
			//We are interested only in the information about artist, located in the 3-th column
			//(counting from 0).
			JavaRDD<String> words = input.flatMap(line-> {
				String[] parts = line.split("\t");
				return Arrays.asList(parts[3]).iterator();
			});
			
			JavaPairRDD<String, Integer> wordOne = words.mapToPair(word -> {
				return new Tuple2<String,Integer>(word,new Integer(1));
			});

			JavaPairRDD<String, Integer> articCount = wordOne.reduceByKey((a,b) ->  a + b);

			articCount.saveAsTextFile("/tmp/userclicks.txt");
			context.close();

		}
}
