package org.apache.accumulo.core.file.rfile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Writable;

public class BlockStats implements Writable {
  
  private static ColumnVisibility emptyVisibility = new ColumnVisibility();
  private static int maxVisibilityLength = 100;
  
  public BlockStats(long minTimestamp, long maxTimestamp, ColumnVisibility minimumVisibility, int entries) {
    this.minTimestamp = minTimestamp;
    this.maxTimestamp = maxTimestamp;
    this.minimumVisibility = minimumVisibility;
    this.entries = entries;
    this.version = RFile.RINDEX_VER_7;
  }
  
  long minTimestamp = Long.MAX_VALUE;
  long maxTimestamp = Long.MIN_VALUE;
  ColumnVisibility minimumVisibility = null;
  int entries = 0;
  final int version;
  
  public void updateBlockStats(Key key, Value value) {
    if (minTimestamp > key.getTimestamp())
      minTimestamp = key.getTimestamp();
    if (maxTimestamp < key.getTimestamp())
      maxTimestamp = key.getTimestamp();
    entries++;
    if (key.getColumnVisibilityData().length() > 0)
      combineVisibilities(new ColumnVisibility(key.getColumnVisibility()));
    else
      combineVisibilities(emptyVisibility);
  }
  
  private void combineVisibilities(ColumnVisibility other) {
    if (minimumVisibility == null)
      minimumVisibility = other;
    else
      minimumVisibility = minimumVisibility.or(other);
  }
  
  public void updateBlockStats(BlockStats other) {
    this.entries += other.entries;
    if (this.minTimestamp > other.minTimestamp)
      this.minTimestamp = other.minTimestamp;
    if (this.maxTimestamp < other.maxTimestamp)
      this.maxTimestamp = other.maxTimestamp;
    combineVisibilities(other.minimumVisibility);
  }
  
  public BlockStats() {
    minTimestamp = Long.MAX_VALUE;
    maxTimestamp = Long.MIN_VALUE;
    minimumVisibility = null;
    entries = 0;
    version = RFile.RINDEX_VER_7;
  }
  
  public BlockStats(int version) {
    this.version = version;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    if (version == RFile.RINDEX_VER_7) {
      minTimestamp = in.readLong();
      maxTimestamp = in.readLong();
      int visibilityLength = in.readInt();
      if (visibilityLength >= 0) {
        byte[] visibility = new byte[visibilityLength];
        in.readFully(visibility);
        minimumVisibility = new ColumnVisibility(visibility);
      } else {
        minimumVisibility = null;
      }
    } else {
      minTimestamp = Long.MIN_VALUE;
      maxTimestamp = Long.MAX_VALUE;
      minimumVisibility = null;
    }
    entries = in.readInt();
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    if (version == RFile.RINDEX_VER_7) {
      out.writeLong(minTimestamp);
      out.writeLong(maxTimestamp);
      if (minimumVisibility == null)
        out.writeInt(-1);
      else {
        byte[] visibility = minimumVisibility.getExpression();
        if (visibility.length > maxVisibilityLength) {
          System.out.println("expression too large: "+toString());
          out.writeInt(0);
        } else {
          out.writeInt(visibility.length);
          out.write(visibility);
        }
      }
    }
    out.writeInt(entries);
  }
  
  @Override
  public String toString() {
    return "{"+entries+";"+minTimestamp+";"+maxTimestamp+";"+minimumVisibility+"}";
  }
}
