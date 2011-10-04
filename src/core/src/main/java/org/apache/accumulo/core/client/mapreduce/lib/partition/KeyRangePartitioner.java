package org.apache.accumulo.core.client.mapreduce.lib.partition;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * Hadoop partitioner that uses ranges based on row keys, and optionally
 * sub-bins based on hashing.
 */
public class KeyRangePartitioner extends Partitioner<Key, Writable> implements Configurable
{
    private RangePartitioner rp = new RangePartitioner();

    @Override
    public int getPartition(Key key, Writable value, int numPartitions)
    {
        return rp.getPartition(key.getRow(), value, numPartitions);
    }

    @Override
    public Configuration getConf()
    {
        return rp.getConf();
    }

    @Override
    public void setConf(Configuration conf)
    {
        rp.setConf(conf);
    }

    /**
     * Sets the hdfs file name to use, containing a newline separated list of
     * Base64 encoded split points that represent ranges for partitioning
     */
    public static void setSplitFile(JobContext job, String file)
    {
        RangePartitioner.setSplitFile(job, file);
    }

    /**
     * Sets the number of random sub-bins per range
     */
    public static void setNumSubBins(JobContext job, int num)
    {
        RangePartitioner.setNumSubBins(job, num);
    }
}
