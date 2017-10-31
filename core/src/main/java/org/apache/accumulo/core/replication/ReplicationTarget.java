/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.replication;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Container for where some work needs to be replicated
 */
public class ReplicationTarget implements Writable {

  private String peerName;
  private String remoteIdentifier;
  private Table.ID sourceTableId;

  public ReplicationTarget() {}

  public ReplicationTarget(String peerName, String remoteIdentifier, Table.ID sourceTableId) {
    this.peerName = peerName;
    this.remoteIdentifier = remoteIdentifier;
    this.sourceTableId = sourceTableId;
  }

  public String getPeerName() {
    return peerName;
  }

  public void setPeerName(String peerName) {
    this.peerName = peerName;
  }

  public String getRemoteIdentifier() {
    return remoteIdentifier;
  }

  public void setRemoteIdentifier(String remoteIdentifier) {
    this.remoteIdentifier = remoteIdentifier;
  }

  public Table.ID getSourceTableId() {
    return sourceTableId;
  }

  public void setSourceTableId(Table.ID sourceTableId) {
    this.sourceTableId = sourceTableId;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (null == peerName) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      WritableUtils.writeString(out, peerName);
    }

    if (null == remoteIdentifier) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      WritableUtils.writeString(out, remoteIdentifier);
    }

    if (null == sourceTableId) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      WritableUtils.writeString(out, sourceTableId.canonicalID());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    if (in.readBoolean()) {
      this.peerName = WritableUtils.readString(in);
    }
    if (in.readBoolean()) {
      this.remoteIdentifier = WritableUtils.readString(in);
    }
    if (in.readBoolean()) {
      this.sourceTableId = Table.ID.of(WritableUtils.readString(in));
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(64);
    sb.append("Remote Name: ").append(peerName).append(" Remote identifier: ").append(remoteIdentifier).append(" Source Table ID: ").append(sourceTableId);
    return sb.toString();
  }

  @Override
  public int hashCode() {
    return peerName.hashCode() ^ remoteIdentifier.hashCode() ^ sourceTableId.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ReplicationTarget) {
      ReplicationTarget other = (ReplicationTarget) o;

      return peerName.equals(other.peerName) && remoteIdentifier.equals(other.remoteIdentifier) && sourceTableId.equals(other.sourceTableId);
    }

    return false;
  }

  /**
   * Deserialize a ReplicationTarget
   *
   * @param t
   *          Serialized copy
   * @return the deserialized version
   */
  public static ReplicationTarget from(Text t) {
    ReplicationTarget target = new ReplicationTarget();
    DataInputBuffer buffer = new DataInputBuffer();
    buffer.reset(t.getBytes(), t.getLength());

    try {
      target.readFields(buffer);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return target;
  }

  /**
   * Deserialize a ReplicationTarget
   *
   * @param s
   *          Serialized copy
   * @return the deserialized version
   */
  public static ReplicationTarget from(String s) {
    ReplicationTarget target = new ReplicationTarget();
    DataInputBuffer buffer = new DataInputBuffer();
    buffer.reset(s.getBytes(UTF_8), s.length());

    try {
      target.readFields(buffer);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return target;
  }

  /**
   * Convenience method to serialize a ReplicationTarget to {@link Text} using the {@link Writable} methods without caring about performance penalties due to
   * excessive object creation
   *
   * @return The serialized representation of the object
   */
  public Text toText() {
    DataOutputBuffer buffer = new DataOutputBuffer();

    try {
      this.write(buffer);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Text t = new Text();
    // Throw it in a text for the mutation
    t.set(buffer.getData(), 0, buffer.getLength());
    return t;
  }
}
