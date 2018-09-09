package de.fraunhofer.iais.kd.bda.spark;

import java.io.Serializable;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class UserSet implements Serializable {
	private HashSet<String> dataSet = new HashSet<String>();
	
	public UserSet add(String username) {
		dataSet.add(username);
		return this;
	}
	
	public UserSet add(UserSet userSet) {
		this.dataSet.addAll(userSet.dataSet);
		return this;
	}
	
	public double distanceTo(UserSet other) {
		HashSet<String> intersection = new HashSet<String>(this.dataSet);
		intersection.retainAll(other.dataSet);
		
		HashSet<String> union = new HashSet<String>(this.dataSet);
		union.addAll(other.dataSet);
		
		return 1 - (intersection.size()/(double)union.size());
	}
	
	public String toMinHashSignature() {
		StringBuffer minHashSign = new StringBuffer();
		for (int j = 0; j < 20; j++) {
			ArrayList<Long> allHashes = new ArrayList<Long>();
			final int i = j;
			this.dataSet.forEach(elem -> {
				long newHash = Basic.hash(i, elem);
				allHashes.add(newHash);
			});
			long currentMin = Collections.min(allHashes);
			String curMin = Long.toString(currentMin);
			minHashSign.append(curMin);
			minHashSign.append(", ");
		}
		minHashSign.deleteCharAt(minHashSign.length() - 1);
		minHashSign.deleteCharAt(minHashSign.length() - 1);
		return minHashSign.toString();
	}
	
	public ArrayList<Long> getMinHash() {
		ArrayList<Long> minHashSign = new ArrayList<Long>();
		for (int j = 0; j < 20; j++) {
			ArrayList<Long> allHashes = new ArrayList<Long>();
			final int i = j;
			this.dataSet.forEach(elem -> {
				long newHash = Basic.hash(i, elem);
				allHashes.add(newHash);
			});
			long currentMin = Collections.min(allHashes);
			minHashSign.add(currentMin);
		}
		return minHashSign;
	}
	
	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("[");
		this.dataSet.forEach(e -> {
				buffer.append(e);
				buffer.append(", ");
			});
		buffer.deleteCharAt(buffer.length() - 1);
		buffer.deleteCharAt(buffer.length() - 1);
		buffer.append("]");
		return buffer.toString();
	}
	
	/**
	 * Mark Knopfler, [ user_000218, user_000934, user_000313, user_000577, user_000017, 
	 * 					user_000655, user_000011, user_000530, user_000095, user_000691, 
	 * 					user_000490, user_000705, user_000906, user_000426, user_000349, 
	 * 					user_000603, user_000702, user_000427, user_000407, user_000269, 
	 * 					user_000423, user_000666, user_000348, user_000986, user_000002, 
	 * 					user_000861, user_000260, user_000261, user_000937 ]
	 * 
	 * Mark Knopfler & Eric Clapton, [ user_000836 ]
	 * Mark Knopfler & Chet Atkins, [ user_000918 ]
	 * Mark Knopfler & Emmylou Harris, [ user_000532, user_000500, user_000705, user_000914, 
	 * 									 user_000162, user_000680, user_000691 ]
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

		articCount.saveAsTextFile("/tmp/userset.txt");
		context.close();

	}
}
