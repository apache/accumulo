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
package org.apache.accumulo.server.conf.codec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.StringJoiner;

/**
 * Serialization metadata to allow for evolution of the encoding used for property storage. This
 * info is expected to be stored first in the serialization and uncompressed so that the handling of
 * subsequent fields and data can be processed correctly and without additional processing.
 * <p>
 * Instances of this class are immutable.
 */
public class EncodingOptions {

  // Adding an encoding version must be done as an addition. Do not change or delete previous
  // version numbers - versions 999 and above reserved for testing
  public static final int EncodingVersion_1_0 = 1;

  private final int encodingVersion;
  private final boolean compress;

  EncodingOptions(final int encodingVersion, final boolean compress) {
    this.encodingVersion = encodingVersion;
    this.compress = compress;
  }

  /**
   * Instantiate encoding options to use version 1.0 encoding settings.
   *
   * @param compress when true compress the property map.
   * @return the encoding options.
   */
  public static EncodingOptions V1_0(final boolean compress) {
    return new EncodingOptions(EncodingVersion_1_0, compress);
  }

  /**
   * Instantiate an instance of EncodingOptions reading the values from an input stream. Typically,
   * the stream will be obtained from reading a byte array from a data store and then creating a
   * stream that reads from that array,
   *
   * @param dis a data input stream
   * @throws IOException if an exception occurs reading from the input stream.
   */
  public static EncodingOptions fromDataStream(final DataInputStream dis) throws IOException {
    return new EncodingOptions(dis.readInt(), dis.readBoolean());
  }

  /**
   * Write the values to a data stream.
   *
   * @param dos a data output stream
   * @throws IOException if an exception occurs writing the data stream.
   */
  public void encode(final DataOutputStream dos) throws IOException {
    dos.writeInt(encodingVersion);
    dos.writeBoolean(compress);
  }

  /**
   * get the encoding version of the instance,
   *
   * @return the encoding version
   */
  public int getEncodingVersion() {
    return encodingVersion;
  }

  /**
   * get if the compress is set.
   *
   * @return true if the payload is compressed, false if not.
   */
  public boolean isCompressed() {
    return compress;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", EncodingOptions.class.getSimpleName() + "[", "]")
        .add("encodingVersion=" + encodingVersion).add("compress=" + compress).toString();
  }
}
