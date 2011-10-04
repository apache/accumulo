package org.apache.accumulo.server.master.state;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.data.KeyExtent;
import org.apache.hadoop.io.Writable;


/**
 * Information about the current merge/rangeDelete. 
 *
 * Writable to serialize for zookeeper and the Tablet
 */

public class MergeInfo implements Writable {

    public enum Operation {
        MERGE,
        DELETE,
    }
    MergeState state = MergeState.NONE;
    KeyExtent range;
    Operation operation = Operation.MERGE;
    
    public MergeInfo() {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        range = new KeyExtent();
        range.readFields(in);
        state = MergeState.values()[in.readInt()];
        operation = Operation.values()[in.readInt()];
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        range.write(out);
        out.writeInt(state.ordinal());
        out.writeInt(operation.ordinal());
    }
    

    public MergeInfo(KeyExtent range, Operation op) {
        this.range = range;
        this.operation = op;
    }

    public MergeState getState() {
        return state;
    }
    public KeyExtent getRange() {
        return range;
    }
    public Operation getOperation() {
        return operation;
    }

    public void setState(MergeState state) {
        this.state = state;
    }
    
    public boolean isDelete() {
        return this.operation.equals(Operation.DELETE);
    }
    
    public boolean needsToBeChopped(KeyExtent extent) {
        // During a delete, the block after the merge will be stretched to cover the deleted area.
        // Therefore, it needs to be chopped
        if (!extent.getTableId().equals(range.getTableId()))
            return false;
        if (isDelete())
            return extent.getPrevEndRow() != null && extent.getPrevEndRow().equals(range.getEndRow());
        else
            return this.range.overlaps(extent);
    }
    
    public boolean overlaps(KeyExtent extent) {
        boolean result = this.range.overlaps(extent);
        if (!result && needsToBeChopped(extent))
            return true;
        return result;
    }
    
    public String toString() {
        if (!state.equals(MergeState.NONE))
            return "Merge " + operation.toString() + " of " + range + " State: " + state;
        return "No Merge in progress";
    }
}

