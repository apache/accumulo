package org.apache.accumulo.examples.mapreduce.bulk;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class GenerateTestData {

	public static void main(String[] args) throws IOException {
		int startRow = Integer.parseInt(args[0]);
		int numRows = Integer.parseInt(args[1]);
		String outputFile = args[2];
		
		Configuration conf = CachedConfiguration.getInstance();
		FileSystem fs = FileSystem.get(conf);
		
		PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(new Path(outputFile))));
		
		for(int i = 0; i < numRows; i++){
			out.println(String.format("row_%08d\tvalue_%08d", i+startRow,i+startRow));
		}

		out.close();
	}

}
