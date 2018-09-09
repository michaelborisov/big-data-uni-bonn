package de.fraunhofer.iais.kd.bda.spark;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ClusterModelFactory {

	private static final boolean skipHeader = true;
	private static final double eps = 0.0000001d;

	public static ClusterModel readFromCsvResource(String file) throws FileNotFoundException {
		ClusterModel retval = new ClusterModel();

//		InputStream inStream = null;
//		inStream = ClusterModel.class.getClassLoader()
//				.getResourceAsStream(file);
//		InputStreamReader isr = new InputStreamReader(inStream);
//		BufferedReader br = new BufferedReader(isr);
		
		
		 FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		try {
			String line;
			if (skipHeader) {
				br.readLine();
			}
			while ((line = br.readLine()) != null) {
				String[] vals = line.split(",");
				String key = vals[0];
				StringBuilder sb = new StringBuilder();
				for (int i = 1; i < vals.length - 1; ++i) {
					//int dval = (int) Double.parseDouble(vals[i]);
					sb.append(vals[i]);
					sb.append(',');
				}
				//int dval = (int) Double.parseDouble(vals[vals.length - 1]);
				sb.append(vals[vals.length - 1]);

				retval.put(key, sb.toString());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return retval;

	}
}
