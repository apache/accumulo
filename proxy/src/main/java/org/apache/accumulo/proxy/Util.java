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
package org.apache.accumulo.proxy;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.accumulo.proxy.thrift.IteratorSetting;
import org.apache.accumulo.proxy.thrift.Key;

public class Util {

  private static Random random = new Random(0);

  public static String randString(int numbytes) {
    return new BigInteger(numbytes * 5, random).toString(32);
  }

  public static ByteBuffer randStringBuffer(int numbytes) {
    return ByteBuffer.wrap(new BigInteger(numbytes * 5, random).toString(32).getBytes(UTF_8));
  }

  public static IteratorSetting iteratorSetting2ProxyIteratorSetting(org.apache.accumulo.core.client.IteratorSetting is) {
    return new IteratorSetting(is.getPriority(), is.getName(), is.getIteratorClass(), is.getOptions());
  }

  public static Key toThrift(org.apache.accumulo.core.data.Key key) {
    Key pkey = new Key(ByteBuffer.wrap(key.getRow().getBytes()), ByteBuffer.wrap(key.getColumnFamily().getBytes()), ByteBuffer.wrap(key.getColumnQualifier()
        .getBytes()), ByteBuffer.wrap(key.getColumnVisibility().getBytes()));
    pkey.setTimestamp(key.getTimestamp());
    return pkey;
  }

  public static org.apache.accumulo.core.data.Key fromThrift(Key pkey) {
    if (pkey == null)
      return null;
    return new org.apache.accumulo.core.data.Key(deNullify(pkey.getRow()), deNullify(pkey.getColFamily()), deNullify(pkey.getColQualifier()),
        deNullify(pkey.getColVisibility()), pkey.getTimestamp());
  }

  protected static byte[] deNullify(byte[] in) {
    if (in == null)
      return new byte[0];
    else
      return in;
  }
}
