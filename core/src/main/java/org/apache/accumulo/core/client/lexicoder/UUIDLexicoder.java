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
package org.apache.accumulo.core.client.lexicoder;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;

import org.apache.accumulo.core.clientImpl.lexicoder.FixedByteArrayOutputStream;

/**
 * A lexicoder for a UUID that maintains its lexicographic sorting order.
 *
 * @since 1.6.0
 */
public class UUIDLexicoder extends AbstractLexicoder<UUID> {

  /**
   * {@inheritDoc}
   *
   * @see <a href="https://www.ietf.org/rfc/rfc4122.txt"> RFC 4122: A Universally Unique IDentifier
   *      (UUID) URN Namespace, "Rules for Lexical Equivalence" in Section 3.</a>
   */
  @Override
  public byte[] encode(UUID uuid) {
    try {
      byte[] ret = new byte[16];
      DataOutputStream out = new DataOutputStream(new FixedByteArrayOutputStream(ret));

      out.writeLong(uuid.getMostSignificantBits() ^ 0x8000000000000000L);
      out.writeLong(uuid.getLeastSignificantBits() ^ 0x8000000000000000L);

      out.close();

      return ret;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public UUID decode(byte[] b) {
    // This concrete implementation is provided for binary compatibility, since the corresponding
    // superclass method has type-erased return type Object. See ACCUMULO-3789 and #1285.
    return super.decode(b);
  }

  @Override
  protected UUID decodeUnchecked(byte[] b, int offset, int len) {
    try {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(b, offset, len));
      return new UUID(in.readLong() ^ 0x8000000000000000L, in.readLong() ^ 0x8000000000000000L);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
