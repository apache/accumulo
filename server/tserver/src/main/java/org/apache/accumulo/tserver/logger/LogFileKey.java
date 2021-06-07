/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Base64;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class LogFileKey implements WritableComparable<LogFileKey> {

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
    if (l < 0)
      return -1;
    if (l > 0)
      return 1;
    return 0;
  }

  @Override
  public int compareTo(LogFileKey o) {
    if (eventType(this.event) != eventType(o.event)) {
      return eventType(this.event) - eventType(o.event);
    }
    if (this.event == OPEN)
      return 0;
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
   * of 3 fields: EventNum + tabletID + seq (separated by underscore). The EventNum is the integer
   * returned by eventType(). The column family is always the event. The column qualifier is
   * dependent of the type of event and could be empty.
   *
   * <pre>
   *     Key Schema:
   *     Row = EventNum_tabletID_seq
   *     Family = event
   *     Qualifier = tserverSession OR filename OR KeyExtent
   * </pre>
   */
  public Key toKey() throws IOException {
    String row = "";
    int eventNum = eventType(event);
    String family = event.name();
    String qual = "";
    switch (event) {
      case OPEN:
        row = formatRow(eventNum, 0, 0);
        qual = tserverSession;
        break;
      case COMPACTION_START:
        row = formatRow(eventNum, tabletId, seq);
        if (filename != null)
          qual = filename;
        break;
      case MUTATION:
      case MANY_MUTATIONS:
      case COMPACTION_FINISH:
        row = formatRow(eventNum, tabletId, seq);
        break;
      case DEFINE_TABLET:
        row = formatRow(eventNum, tabletId, seq);
        // Base64 encode KeyExtent
        DataOutputBuffer buffer = new DataOutputBuffer();
        tablet.writeTo(buffer);
        qual = Base64.getEncoder().encodeToString(copyOf(buffer.getData(), buffer.getLength()));
        buffer.close();
        break;
    }
    return new Key(new Text(row), new Text(family), new Text(qual));
  }

  // format row = 1_000001_0000000001
  private String formatRow(int eventNum, int tabletId, long seq) {
    return String.format("%d_%06d_%010d", eventNum, tabletId, seq);
  }

  /**
   * Create LogFileKey from row. Follows schema defined by {@link #toKey()}
   */
  public static LogFileKey fromKey(Key key) throws IOException {
    var logFileKey = new LogFileKey();
    String[] rowParts = key.getRow().toString().split("_");
    int tabletId = Integer.parseInt(rowParts[1]);
    long seq = Long.parseLong(rowParts[2]);
    String qualifier = key.getColumnQualifier().toString();

    logFileKey.tabletId = tabletId;
    logFileKey.seq = seq;
    logFileKey.event = LogEvents.valueOf(key.getColumnFamilyData().toString());

    // handle special cases of what is stored in the qualifier
    switch (logFileKey.event) {
      case OPEN:
        logFileKey.tserverSession = qualifier;
        break;
      case COMPACTION_START:
        logFileKey.filename = qualifier;
        break;
      case DEFINE_TABLET:
        // decode Base64 KeyExtent
        DataInputBuffer buffer = new DataInputBuffer();
        byte[] bytes = Base64.getDecoder().decode(qualifier);
        buffer.reset(bytes, bytes.length);
        logFileKey.tablet = KeyExtent.readFrom(buffer);
        buffer.close();
        break;
    }

    return logFileKey;
  }
}
