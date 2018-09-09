package de.fraunhofer.iais.kd.bda.spark;

import java.io.Serializable;
import java.util.HashSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class MinHashAggregator implements Serializable {
	/**
	 * The communication overhead is lower than Task 3, as the version in Task 3 has
	 * an additional step of mapToPair, which communicates the users again.
	 */

	private static final long serialVersionUID = 1L;
	private static final int n = 20;
	private long[] hashs;

	// add one user's hash value
	public MinHashAggregator add(String user) {
		if (hashs == null) {
			hashs = new long[n];
			for (int i = 0; i < n; i++)
				hashs[i] = Basic.hash(i, user);

		} else {
			long newHash;
			for (int i = 0; i < n; i++) {
				newHash = Basic.hash(i, user);
				if (hashs[i] > newHash)
					hashs[i] = newHash;
			}
		}
		return this;
	}

	// add another aggregator
	public MinHashAggregator combine(MinHashAggregator other) {
		if (hashs == null)
			hashs = other.hashs;
		else if (other.hashs != null) {
			long newHash;
			for (int i = 0; i < n; i++) {
				newHash = other.hashs[i];
				if (hashs[i] > newHash)
					hashs[i] = newHash;
			}
		}
		return this;
	}

	@Override
	public String toString() {
		StringBuffer minHashSign = new StringBuffer();
		minHashSign.append(" ");
		if (hashs != null)
			for (long hash : hashs) {
				minHashSign.append(hash);
				minHashSign.append(", ");
			}
		if (minHashSign.length() > 1) {
			minHashSign.deleteCharAt(minHashSign.length() - 1);
			minHashSign.deleteCharAt(minHashSign.length() - 1);
		}
		return minHashSign.toString();
	}

	Double distanceTo(MinHashAggregator other) {
//		HashSet<Long> set1 = new HashSet<Long>();
//		for(int i=0; i < this.hashs.length; i ++) {
//			set1.add(this.hashs[i]);
//		}
//		
//		HashSet<Long> set2 = new HashSet<Long>();
//		for(int i=0; i < other.hashs.length; i ++) {
//			set2.add(other.hashs[i]);
//		}
//		HashSet<Long> intersection = new HashSet<Long>(set1);
//		intersection.retainAll(set2);
//		
//		HashSet<Long> union = new HashSet<Long>(set1);
//		union.addAll(set2);
//		
//		if(union.size() == 0) {
//			System.out.println("Error!");
//			System.exit(1);
//		}
		int equalEntries = 0;
		for(int i=0; i < this.hashs.length; i++) {
			if(this.hashs[i] == other.hashs[i]) {
				equalEntries++;
			}
		}
		
		return 1 - (equalEntries/(double)this.hashs.length);
	}
	
	public static void main(String[] args) {
		final double distanceLimit = 0.65;
		final String targetArt = "Mark Knopfler";
		
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

		// Mark's minhash signature
		MinHashAggregator targetArtUser = artminhashs.collectAsMap().get(targetArt);
		
		// Calculate each artists distance to Mark and filter by the limit 0.65
		JavaPairRDD<String, Double> artDistances = artminhashs.mapToPair((artUser) -> {
			return new Tuple2<String, Double>(artUser._1, artUser._2.distanceTo(targetArtUser));
		}).filter((artDistance) -> (artDistance._2 < distanceLimit));

		artDistances.saveAsTextFile("/tmp/closetomark_minhash.txt");
		context.close();

	}

}
