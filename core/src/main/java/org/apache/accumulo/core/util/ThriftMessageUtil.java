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
package org.apache.accumulo.core.util;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;

/**
 * Serializes and deserializes Thrift messages to and from byte arrays. This class is not
 * thread-safe, external synchronization is necessary if it is used concurrently.
 */
public class ThriftMessageUtil {

  private final int initialCapacity;

  private final TMemoryInputTransport inputTransport;
  private final TCompactProtocol inputProtocol;

  public ThriftMessageUtil() {
    this(64);
  }

  public ThriftMessageUtil(int initialCapacity) {
    // TODO does this make sense? better to push this down to the serialize method (accept the
    // transport as an argument)?
    this.initialCapacity = initialCapacity;
    this.inputTransport = new TMemoryInputTransport();
    this.inputProtocol = new TCompactProtocol(inputTransport);
  }

  /**
   * Convert the {@code msg} to a byte array representation
   *
   * @param msg
   *          The message to serialize
   * @return The serialized message
   * @throws IOException
   *           When serialization fails
   */
  public ByteBuffer serialize(TBase<?,?> msg) throws IOException {
    requireNonNull(msg);
    TMemoryBuffer transport = new TMemoryBuffer(initialCapacity);
    TProtocol protocol = new TCompactProtocol(transport);
    try {
      msg.write(protocol);
    } catch (TException e) {
      throw new IOException(e);
    }
    return ByteBuffer.wrap(transport.getArray(), 0, transport.length());
  }

  /**
   * Assumes the entire contents of the byte array compose the serialized {@code instance}
   *
   * @see #deserialize(byte[], int, int, TBase)
   */
  public <T extends TBase<?,?>> T deserialize(byte[] serialized, T instance) throws IOException {
    return deserialize(serialized, 0, serialized.length, instance);
  }

  /**
   * Deserializes a message into the provided {@code instance} from {@code serialized}
   *
   * @param serialized
   *          The serialized representation of the object
   * @param instance
   *          An instance of the object to reconstitute
   * @return The reconstituted instance provided
   * @throws IOException
   *           When deserialization fails
   */
  public <T extends TBase<?,?>> T deserialize(byte[] serialized, int offset, int length, T instance)
      throws IOException {
    requireNonNull(instance);
    inputTransport.reset(serialized, offset, length);
    try {
      instance.read(inputProtocol);
    } catch (TException e) {
      throw new IOException(e);
    }
    return instance;
  }
}
