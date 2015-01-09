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
package org.apache.accumulo.core.client.lexicoder;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;

import org.apache.accumulo.core.client.lexicoder.impl.FixedByteArrayOutputStream;
import org.apache.accumulo.core.iterators.ValueFormatException;

/**
 * A lexicoder for a UUID that maintains its lexicographic sorting order.
 *
 * @since 1.6.0
 */
public class UUIDLexicoder implements Lexicoder<UUID> {

  /**
   * @see <a href="http://www.ietf.org/rfc/rfc4122.txt"> RFC 4122: A Universally Unique IDentifier (UUID) URN Namespace, "Rules for Lexical Equivalence" in
   *      Section 3.</a>
   */
  @Override
  public byte[] encode(UUID uuid) {
    try {
      byte ret[] = new byte[16];
      DataOutputStream out = new DataOutputStream(new FixedByteArrayOutputStream(ret));

      out.writeLong(uuid.getMostSignificantBits() ^ 0x8000000000000000l);
      out.writeLong(uuid.getLeastSignificantBits() ^ 0x8000000000000000l);

      out.close();

      return ret;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public UUID decode(byte[] b) throws ValueFormatException {
    try {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(b));
      return new UUID(in.readLong() ^ 0x8000000000000000l, in.readLong() ^ 0x8000000000000000l);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
