package org.apache.accumulo.server.test.randomwalk.multitable;

import java.io.IOException;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;


public class CopyTool extends Configured implements Tool
{
	protected final Logger log = Logger.getLogger(this.getClass());
		
    public static class SeqMapClass extends Mapper<Key, Value, Text, Mutation>
    {
        public void map(Key key, Value val, Context output) throws IOException, InterruptedException
        {
        	Mutation m = new Mutation(key.getRow());
        	m.put(key.getColumnFamily(), key.getColumnQualifier(), val);
        	output.write(null, m);
        }
    }

    public int run(String[] args) throws Exception
    {
        Job job = new Job(getConf(), this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        
        if (job.getJar() == null) {
        	log.error("M/R requires a jar file!  Run mvn package.");
        	return 1;
        }

        job.setInputFormatClass(AccumuloInputFormat.class);
        AccumuloInputFormat.setInputInfo(job, args[0], args[1].getBytes(), args[2], new Authorizations());
        AccumuloInputFormat.setZooKeeperInstance(job, args[3], args[4]);
        
        job.setMapperClass(SeqMapClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Mutation.class);
        
        job.setNumReduceTasks(0);
        
        job.setOutputFormatClass(AccumuloOutputFormat.class);
        AccumuloOutputFormat.setOutputInfo(job, args[0], args[1].getBytes(), true, args[5]);
        AccumuloOutputFormat.setZooKeeperInstance(job, args[3], args[4]);

        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }
}