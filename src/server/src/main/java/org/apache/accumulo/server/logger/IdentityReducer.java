package org.apache.accumulo.server.logger;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

// No longer needed after hadoop 0.20.1 
public class IdentityReducer extends Reducer<WritableComparable<?>, Writable, WritableComparable<?>, Writable> {
    public IdentityReducer() { super(); }
}
