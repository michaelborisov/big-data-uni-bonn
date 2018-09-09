package de.fraunhofer.iais.kd.bda.spark;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

public class MinHashClusterModel implements Serializable{
	ClusterModel clusterModel = null;
	HashMap<String, MinHashAggregator> hashMap = new HashMap<String, MinHashAggregator>();
	public MinHashClusterModel(String filePath) {
		try {
			clusterModel = ClusterModelFactory.readFromCsvResource(filePath);
			constructIdUserSetHashMap();
		}catch(FileNotFoundException ex) {
			ex.printStackTrace();
		}
	}
	
	// Constructs minhash signature for representatives of clusters.
	// This signatures are stored in Map with the key equal to Cluster key
	void constructIdUserSetHashMap() {
		Set<String> keys = clusterModel.getKeys();
		keys.forEach(key -> {
			String usersStr = clusterModel.get(key);
			String[] users = usersStr.split(",");
			MinHashAggregator minHashAgg = new MinHashAggregator();
			for(String user: users) {
				minHashAgg.add(user);
			}
			hashMap.put(key, minHashAgg);
			
		});
	}
	
	// Iterates over all minhash signatures of representatives of clusters and finds 
	// the closest.
	String findClosestCluster(MinHashAggregator minhash) {
		double minDistance = Double.MAX_VALUE;
		String minDistanceKey = null;
		
		Set<String> keys = clusterModel.getKeys();
		for(String key : keys) {
			MinHashAggregator possible = hashMap.get(key);
			double possibleMinDistance = possible.distanceTo(minhash);
			if(minDistance > possibleMinDistance) {
				minDistance = possibleMinDistance;
				minDistanceKey = key;
			}
			
		}
		return minDistanceKey;
	}
}
