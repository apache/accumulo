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
package org.apache.accumulo.server.conf.codec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Serialization metadata used to verify cached values match stored values. Storing the metadata
 * with the properties allows for comparison of properties and can be used to ensure that vales
 * being written to the backend store have not changed. This metadata should be written / appear
 * early in the encoded bytes and be uncompressed so that decisions can be made that may make
 * deserialization unnecessary.
 * <p>
 * Note: Avoid using -1 because that has significance in ZooKeeper - writing a ZooKeeper node with a
 * version of -1 disables the ZooKeeper expected version checking and just overwrites the node.
 * <p>
 * Instances of this class are immutable.
 */
public class VersionInfo {

  // flag value for initialization - on store the version will be 0.
  public static final int NO_VERSION = -2;

  private static final DateTimeFormatter tsFormatter =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC));

  private final int dataVersion;
  private final Instant timestamp;

  /**
   * Instantiate an instance with the provided version and timestamp.
   *
   * @param dataVersion
   *          the data version used for comparisons with the ZooKeeper node version or
   *          {@link #NO_VERSION} if it has not yet been committed to the backend store.
   * @param timestamp
   *          the timestamp this version was created or updated.
   */
  private VersionInfo(final int dataVersion, final Instant timestamp) {
    this.dataVersion = dataVersion;
    this.timestamp = timestamp;
  }

  /**
   * Instantiate an instance reading from a {@link java.io.DataInputStream}. Normally the underlying
   * data stream would be read from ZooKeeper.
   *
   * @param dis
   *          a {@link java.io.DataInputStream}
   * @throws IOException
   *           if an exception occurs reading from the input stream
   */
  public VersionInfo(final DataInputStream dis) throws IOException {
    dataVersion = dis.readInt();
    timestamp = tsFormatter.parse(dis.readUTF(), Instant::from);
  }

  /**
   * Get the current data version. The version should match the node version of the stored data.
   * This value returned should be used on data writes as the expected version. If the data write
   * fails do to unexpected version, it signals that the node version has changed.
   *
   * @return 0 for initial version, otherwise the data version when the properties were serialized.
   */
  public int getDataVersion() {
    return Math.max(dataVersion, 0);
  }

  /**
   * Calculates the version that should be stored when serialized. The serialized version, when
   * stored, should match the version that will be assigned. This way, data reading the serialized
   * version can compare the stored version with the node version at any time to detect if the node
   * version has been updated.
   * <p>
   * The initialization of the data version to a negative value allows this value to be calculated
   * correctly for the first serialization. On the first store, the expected version will be 0.
   *
   * @return the next version number that should be serialized, or 0 if this is the initial version.
   */
  public int getNextVersion() {
    return Math.max(dataVersion + 1, 0);
  }

  /**
   * Properties are timestamped when the properties are serialized for storage. This is to allow
   * easy comparison of properties that could have been retrieved at different times.
   *
   * @return the timestamp when the properties were serialized.
   */
  public Instant getTimestamp() {
    return timestamp;
  }

  /**
   * Get a String formatted version of the timestamp.
   *
   * @return the timestamp formatted as an ISO time.
   */
  public String getTimestampISO() {
    return tsFormatter.format(timestamp);
  }

  /**
   * Write updated version version info to a {@link java.io.DataOutputStream} the stream is not
   * closed by this method. The info written has the expected data version and a current timestamp.
   * The version info of this instance is not modified - only the serialized version.
   *
   * @param dos
   *          a DataOutputStream
   * @throws IOException
   *           if an exception occurs writing to the stream.
   */
  public void encode(final DataOutputStream dos) throws IOException {
    dos.writeInt(getNextVersion());
    dos.writeUTF(tsFormatter.format(Instant.now()));
  }

  /**
   * Create a human friendly string useful for debugging that is easier to read than toString.
   *
   * @param prettyPrint
   *          if true separate values with new lines.
   * @return a formatted string
   */
  public String print(boolean prettyPrint) {
    return "dataVersion=" + getDataVersion() + (prettyPrint ? "\n" : ", ") + "timestamp="
        + getTimestampISO() + (prettyPrint ? "\n" : ", ");
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", VersionInfo.class.getSimpleName() + "[", "]").add(print(false))
        .toString();
  }

  public static class Builder {

    private int dataVersion = NO_VERSION;
    private Instant timestamp;

    public VersionInfo build() {
      if (Objects.isNull(timestamp)) {
        timestamp = Instant.now();
      }
      return new VersionInfo(dataVersion, timestamp);
    }

    public Builder withTimestamp(final Instant timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Builder withDataVersion(final int dataVersion) {
      this.dataVersion = dataVersion;
      return this;
    }
  }
}
