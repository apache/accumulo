package org.apache.accumulo.core.file.rfile;

import java.util.ArrayList;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class PrintInfo {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		Options opts = new Options();
        Option dumpKeys = new Option("d", "dump", false, "dump the key/value pairs");
        opts.addOption(dumpKeys);

        CommandLine commandLine = new BasicParser().parse(opts, args);
            
		for (String arg : commandLine.getArgs())  {
		    
		    Path path = new Path(arg);
		    CachableBlockFile.Reader _rdr = new CachableBlockFile.Reader(fs, path, conf, null, null);
		    Reader iter = new RFile.Reader(_rdr);

		    iter.printInfo();
		    
		    if (commandLine.hasOption(dumpKeys.getOpt())) {
		        iter.seek(new Range((Key)null, (Key)null), new ArrayList<ByteSequence>(), false);
		        while (iter.hasTop()) {
		            Key key = iter.getTopKey();
		            Value value = iter.getTopValue();
		            System.out.println(key + " -> " + value);
		            iter.next();
		        }
		    }

		    iter.close();
		}
	}
}
