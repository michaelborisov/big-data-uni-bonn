package de.fraunhofer.iais.kd.bda.spark;

public class Basic {
	public static long hash(int i, String s) {
		long[] prime = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173};
		long p = 1009;
		long r = 1000;
		long a = prime[i];
		long b = prime[i+20];
		long x = Math.abs(s.hashCode());
		long res = ((a*x + b) % p) % r;
		return res;
	}
}
