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
import java.util.StringJoiner;

/**
 * Serialization metadata to allow for evolution of the encoding used for property storage. This
 * info is expected to be stored first in the serialization and uncompressed so that the handling of
 * subsequent fields and data can be processed correctly.
 * <p>
 * Instances of this class are immutable.
 */
public class EncodingOptions {

  public static final EncodingOptions COMPRESSED_V1 =
      new EncodingOptions(EncodingOptions.EncodingVersion.V1_0, true);

  public static final EncodingOptions UNCOMPRESSED_V1 =
      new EncodingOptions(EncodingOptions.EncodingVersion.V1_0, false);

  private final EncodingVersion encodingVersion;
  private final boolean compress;

  public EncodingOptions(EncodingVersion encodingVersion, final boolean compress) {
    this.encodingVersion = encodingVersion;
    this.compress = compress;
  }

  public EncodingOptions(final DataInputStream dis) throws IOException {
    encodingVersion = EncodingVersion.byId(dis.readInt());
    compress = dis.readBoolean();
  }

  public void encode(final DataOutputStream dos) throws IOException {
    dos.writeInt(encodingVersion.id);
    dos.writeBoolean(compress);
  }

  public EncodingVersion getEncodingVersion() {
    return encodingVersion;
  }

  public boolean isCompressed() {
    return compress;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", EncodingOptions.class.getSimpleName() + "[", "]")
        .add("encodingVersion=" + encodingVersion).toString();
  }

  /**
   * Provides a strong typing of the known encoding versions and allows the version id to be encoded
   * as an integer. Adding an encoding type must be done as an addition and not change or delete
   * previous versions or numbering to preserve compatibility.
   */
  public enum EncodingVersion {

    INVALID(-1), V1_0(1);

    // a unique id to identify the version
    public final Integer id;

    EncodingVersion(final Integer id) {
      this.id = id;
    }

    /**
     * An integer id for serialization so that version detection / selection on a serialized stream
     * can be done with an integer comparison. If the name was used instead, a string comparison
     * would be needed. This also provides independence from the enum declaration order (if enum
     * ordinal was used.)
     *
     * @param id
     *          the id used in serialization
     * @return the EncodingVersion
     */
    static EncodingVersion byId(final Integer id) {
      switch (id) {
        case 1:
          return V1_0;
        default:
          return INVALID;
      }
    }
  }
}
