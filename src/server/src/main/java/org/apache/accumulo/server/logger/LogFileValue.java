/**
 * 
 */
package org.apache.accumulo.server.logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Writable;


public class LogFileValue implements Writable {
    
    private static final Mutation[] empty = new Mutation[0];
    
    public Mutation[] mutations = empty;
    
    @Override
    public void readFields(DataInput in) throws IOException {
        int count = in.readInt();
        mutations = new Mutation[count];
        for (int i = 0; i < count; i++) {
            mutations[i] = new Mutation();
            mutations[i].readFields(in);
        }
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
            out.writeInt(mutations.length);
            for (int i = 0; i < mutations.length; i++) {
                mutations[i].write(out);
            }
    }
    
    public static void print(LogFileValue value) {
        System.out.println(value.toString());
    }
    
    private static String displayLabels(byte[] labels) {
        String s = new String(labels);
        s = s.replace("&", " & ");
        s = s.replace("|", " | ");
        return s;
    }
    
    public static String format(LogFileValue lfv, int maxMutations) {
        if (lfv.mutations.length == 0)
            return "";
        StringBuilder builder = new StringBuilder();
        builder.append(lfv.mutations.length + " mutations:\n");
        for (int i = 0; i < lfv.mutations.length; i++) {
            if (i >= maxMutations) {
                builder.append("...");
                break;
            }
            Mutation m = lfv.mutations[i];
            builder.append("  " + new String(m.getRow()) + "\n");
            for (ColumnUpdate update : m.getUpdates()) {
                String value = new String(update.getValue());
                builder.append("      " + new String(update.getColumnFamily()) + 
                               ":" + new String(update.getColumnQualifier()) +
                               " " + (update.hasTimestamp() ? "[user]:" : "[system]:") + update.getTimestamp() +
                               " [" + displayLabels(update.getColumnVisibility()) +
                               "] " + (update.isDeleted() ? "<deleted>" : value) + "\n");
            }
        }
        return builder.toString();
    }
    
    
    @Override
    public String toString() {
        return format(this, 5);
    }
    
}