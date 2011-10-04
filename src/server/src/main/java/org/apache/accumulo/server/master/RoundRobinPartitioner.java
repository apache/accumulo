package org.apache.accumulo.server.master;

import org.apache.accumulo.server.logger.LogFileKey;
import org.apache.accumulo.server.logger.LogFileValue;
import org.apache.hadoop.mapreduce.Partitioner;


public class RoundRobinPartitioner extends Partitioner<LogFileKey, LogFileValue> {

    int counter = 0;

    @Override
    public int getPartition(LogFileKey key, LogFileValue value, int numPartitions) {
        // We don't really care if items with the same key stay together: we
        // just want a sort, with the load spread evenly over the reducers
        counter = ++counter % numPartitions;
        return counter;
    }
}
