/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.logger;

import static java.util.Arrays.copyOf;
import static org.apache.accumulo.tserver.logger.LogEvents.DEFINE_TABLET;
import static org.apache.accumulo.tserver.logger.LogEvents.MANY_MUTATIONS;
import static org.apache.accumulo.tserver.logger.LogEvents.MUTATION;
import static org.apache.accumulo.tserver.logger.LogEvents.OPEN;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class LogFileKey implements WritableComparable<LogFileKey> {
  private static final Logger log = LoggerFactory.getLogger(LogFileKey.class);

  public LogEvents event;
  public String filename = null;
  public KeyExtent tablet = null;
  public long seq = -1;
  public int tabletId = -1;
  public static final int VERSION = 2;
  public String tserverSession;

  @Override
  public void readFields(DataInput in) throws IOException {
    int value = in.readByte();
    if (value >= LogEvents.values().length) {
      throw new IOException("Invalid LogEvent type, got ordinal " + value + ", but only know about "
          + LogEvents.values().length + " possible types.");
    }
    event = LogEvents.values()[value];
    switch (event) {
      case OPEN:
        tabletId = in.readInt();
        tserverSession = in.readUTF();
        if (tabletId != VERSION) {
          throw new RuntimeException(String.format(
              "Bad version number for log file: expected %d, but saw %d", VERSION, tabletId));
        }
        break;
      case COMPACTION_FINISH:
        seq = in.readLong();
        tabletId = in.readInt();
        break;
      case COMPACTION_START:
        seq = in.readLong();
        tabletId = in.readInt();
        filename = in.readUTF();
        break;
      case DEFINE_TABLET:
        seq = in.readLong();
        tabletId = in.readInt();
        tablet = KeyExtent.readFrom(in);
        break;
      case MANY_MUTATIONS:
        seq = in.readLong();
        tabletId = in.readInt();
        break;
      case MUTATION:
        seq = in.readLong();
        tabletId = in.readInt();
        break;
      default:
        throw new RuntimeException("Unknown log event type: " + event);
    }

  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(event.ordinal());
    switch (event) {
      case OPEN:
        seq = -1;
        tabletId = -1;
        out.writeInt(VERSION);
        out.writeUTF(tserverSession);
        // out.writeUTF(Accumulo.getInstanceID());
        break;
      case COMPACTION_FINISH:
        out.writeLong(seq);
        out.writeInt(tabletId);
        break;
      case COMPACTION_START:
        out.writeLong(seq);
        out.writeInt(tabletId);
        out.writeUTF(filename);
        break;
      case DEFINE_TABLET:
        out.writeLong(seq);
        out.writeInt(tabletId);
        tablet.writeTo(out);
        break;
      case MANY_MUTATIONS:
        out.writeLong(seq);
        out.writeInt(tabletId);
        break;
      case MUTATION:
        out.writeLong(seq);
        out.writeInt(tabletId);
        break;
      default:
        throw new IllegalArgumentException("Bad value for LogFileEntry type");
    }
  }

  static int eventType(LogEvents event) {
    // Order logs by START, TABLET_DEFINITIONS, COMPACTIONS and then MUTATIONS
    if (event == MUTATION || event == MANY_MUTATIONS) {
      return 3;
    }
    if (event == DEFINE_TABLET) {
      return 1;
    }
    if (event == OPEN) {
      return 0;
    }
    return 2;
  }

  private static int sign(long l) {
    if (l < 0) {
      return -1;
    }
    if (l > 0) {
      return 1;
    }
    return 0;
  }

  @Override
  public int compareTo(LogFileKey o) {
    if (eventType(this.event) != eventType(o.event)) {
      return eventType(this.event) - eventType(o.event);
    }
    if (this.event == OPEN) {
      return 0;
    }
    if (this.tabletId != o.tabletId) {
      return this.tabletId - o.tabletId;
    }
    return sign(this.seq - o.seq);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof LogFileKey) {
      return this.compareTo((LogFileKey) obj) == 0;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return (int) seq;
  }

  @Override
  public String toString() {
    switch (event) {
      case OPEN:
        return String.format("OPEN %s", tserverSession);
      case COMPACTION_FINISH:
        return String.format("COMPACTION_FINISH %d %d", tabletId, seq);
      case COMPACTION_START:
        return String.format("COMPACTION_START %d %d %s", tabletId, seq, filename);
      case MUTATION:
        return String.format("MUTATION %d %d", tabletId, seq);
      case MANY_MUTATIONS:
        return String.format("MANY_MUTATIONS %d %d", tabletId, seq);
      case DEFINE_TABLET:
        return String.format("DEFINE_TABLET %d %d %s", tabletId, seq, tablet);
    }
    throw new RuntimeException("Unknown type of entry: " + event);
  }

  /**
   * Converts LogFileKey to Key. Creates a Key containing all of the LogFileKey fields. The fields
   * are stored so the Key sorts maintaining the legacy sort order. The row of the Key is composed
   * of 3 fields: EventNum + tabletID + seq. The EventNum is the byte returned by eventType(). The
   * column family is always the event. The column qualifier is dependent of the type of event and
   * could be empty.
   *
   * <pre>
   *     Key Schema:
   *     Row = EventNum + tabletID + seq
   *     Family = event
   *     Qualifier = tserverSession OR filename OR KeyExtent
   * </pre>
   */
  public Key toKey() throws IOException {
    byte[] formattedRow;
    String family = event.name();
    var kb = Key.builder();
    switch (event) {
      case OPEN:
        formattedRow = formatRow(0, 0);
        return kb.row(formattedRow).family(family).qualifier(tserverSession).build();
      case COMPACTION_START:
        formattedRow = formatRow(tabletId, seq);
        return kb.row(formattedRow).family(family).qualifier(filename).build();
      case MUTATION:
      case MANY_MUTATIONS:
      case COMPACTION_FINISH:
        return kb.row(formatRow(tabletId, seq)).family(family).build();
      case DEFINE_TABLET:
        formattedRow = formatRow(tabletId, seq);
        DataOutputBuffer buffer = new DataOutputBuffer();
        tablet.writeTo(buffer);
        var q = copyOf(buffer.getData(), buffer.getLength());
        buffer.close();
        return kb.row(formattedRow).family(family).qualifier(q).build();
      default:
        throw new AssertionError("Invalid event type in LogFileKey: " + event);
    }
  }

  /**
   * Get the first byte for the event. The only possible values are 0-4. This is used as the highest
   * byte in the row.
   */
  private byte getEventByte() {
    int eventTypeInteger = eventType(event);
    return (byte) (eventTypeInteger & 0xff);
  }

  /**
   * Get the byte encoded row for this LogFileKey as a Text object.
   */
  private Text formatRow() {
    return new Text(formatRow(tabletId, seq));
  }

  /**
   * Format the row using 13 bytes encoded to allow proper sorting of the RFile Key. The highest
   * byte is for the event number, 4 bytes for the tabletId and 8 bytes for the sequence long. The
   * conversions of integer to byte[] and long to byte[] is similar to what DataOutputStream does
   * for writeInt() and writeLong()
   */
  private byte[] formatRow(int tabletId, long seq) {
    byte eventNum = getEventByte();
    // These will not sort properly when encoded if negative. Negative is not expected currently,
    // defending against future changes and/or bugs.
    Preconditions.checkArgument(eventNum >= 0 && seq >= 0);
    byte[] row = new byte[13];
    // encode the signed integer so negatives will sort properly for tabletId
    int encodedTabletId = tabletId ^ 0x80000000;

    int mask = 0xff; // use a mask of int type to truncate the selected 8 bits
    row[0] = eventNum;
    row[1] = (byte) ((encodedTabletId >>> 24) & mask);
    row[2] = (byte) ((encodedTabletId >>> 16) & mask);
    row[3] = (byte) ((encodedTabletId >>> 8) & mask);
    row[4] = (byte) (encodedTabletId & mask);
    row[5] = (byte) ((seq >>> 56) & mask);
    row[6] = (byte) ((seq >>> 48) & mask);
    row[7] = (byte) ((seq >>> 40) & mask);
    row[8] = (byte) ((seq >>> 32) & mask);
    row[9] = (byte) ((seq >>> 24) & mask);
    row[10] = (byte) ((seq >>> 16) & mask);
    row[11] = (byte) ((seq >>> 8) & mask);
    row[12] = (byte) (seq & mask);

    log.trace("Convert {} {} {} to row {}", event, tabletId, seq, Arrays.toString(row));
    return row;
  }

  /**
   * Extract the tabletId integer from the byte encoded Row. Similar to what DataInputStream does
   * for readInt()
   */
  private static int getTabletId(byte[] row) {
    int mask = 0xff; // use a mask of int type to convert byte to int without sign extension
    // @formatter:off
    int encoded = (row[1] & mask) << 24 |
                  (row[2] & mask) << 16 |
                  (row[3] & mask) <<  8 |
                  (row[4] & mask);
    // @formatter:on
    return encoded ^ 0x80000000;
  }

  /**
   * Extract the sequence long from the byte encoded Row.
   */
  private static long getSequence(byte[] row) {
    long mask = 0xff; // use a mask of long type to convert byte to long without sign extension
    // @formatter:off
    return (row[5]  & mask) << 56 |
           (row[6]  & mask) << 48 |
           (row[7]  & mask) << 40 |
           (row[8]  & mask) << 32 |
           (row[9]  & mask) << 24 |
           (row[10] & mask) << 16 |
           (row[11] & mask) <<  8 |
           (row[12] & mask);
    // @formatter:on
  }

  public static Range toRange(LogFileKey start, LogFileKey end) {
    return new Range(start.formatRow(), end.formatRow());
  }

  /**
   * Create LogFileKey from row. Follows schema defined by {@link #toKey()}
   */
  public static LogFileKey fromKey(Key key) {
    var logFileKey = new LogFileKey();
    byte[] rowParts = key.getRow().getBytes();

    logFileKey.tabletId = getTabletId(rowParts);
    logFileKey.seq = getSequence(rowParts);
    logFileKey.event = LogEvents.valueOf(key.getColumnFamilyData().toString());
    // verify event number in row matches column family
    if (eventType(logFileKey.event) != rowParts[0]) {
      throw new AssertionError("Event in row differs from column family. Key: " + key);
    }
    log.trace("From row {} get {} {} {}", Arrays.toString(rowParts), logFileKey.event,
        logFileKey.tabletId, logFileKey.seq);

    // handle special cases of what is stored in the qualifier
    switch (logFileKey.event) {
      case OPEN:
        logFileKey.tserverSession = key.getColumnQualifierData().toString();
        break;
      case COMPACTION_START:
        logFileKey.filename = key.getColumnQualifierData().toString();
        break;
      case DEFINE_TABLET:
        try (DataInputBuffer buffer = new DataInputBuffer()) {
          byte[] bytes = key.getColumnQualifierData().toArray();
          buffer.reset(bytes, bytes.length);
          logFileKey.tablet = KeyExtent.readFrom(buffer);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        break;
      case COMPACTION_FINISH:
      case MANY_MUTATIONS:
      case MUTATION:
        // nothing to do
        break;
      default:
        throw new AssertionError("Invalid event type in key: " + key);
    }

    return logFileKey;
  }
}
