package org.apache.accumulo.server.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.thrift.TMutation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import static org.apache.accumulo.core.data.Mutation.SERIALIZED_FORMAT.VERSION2;;

/**
 * Mutation that holds system time as computed by the tablet server when not provided by the user.
 */
public class ServerMutation extends Mutation {
  private long systemTime = 0l;
  
  public ServerMutation(TMutation tmutation) {
    super(tmutation);
  }

  public ServerMutation(Text key) {
    super(key);
  }

  public ServerMutation() {
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    // new format writes system time with the mutation
    if (getSerializedFormat() == VERSION2)
      systemTime = WritableUtils.readVLong(in);
    else {
      // old format stored it in the timestamp of each mutation
      for (ColumnUpdate upd : getUpdates()) {
        if (!upd.hasTimestamp()) {
          systemTime = upd.getTimestamp();
          break;
        }
      }
    }
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    WritableUtils.writeVLong(out, systemTime);
  }

  public void setSystemTimestamp(long v) {
    this.systemTime = v;
  }
  
  public long getSystemTimestamp() {
    return this.systemTime;
  }
  
  public List<ColumnUpdate> getUpdates() {
    List<ColumnUpdate> updates = super.getUpdates();
    List<ColumnUpdate> result = new ArrayList<ColumnUpdate>(updates.size());
    for (ColumnUpdate update : updates) {
      result.add(new ServerColumnUpdate(update, this));
    }
    return result;
  }

  @Override
  public long estimatedMemoryUsed() {
    return super.estimatedMemoryUsed() + 8;
  }

}
