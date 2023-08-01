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
package org.apache.accumulo.core.tasks;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransportException;

public interface ThriftSerialization<T extends TBase<?,?>> {

  TBinaryProtocol.Factory FACTORY = new TBinaryProtocol.Factory();

  ThreadLocal<TSerializer> SERIALIZER = new ThreadLocal<>() {
    @Override
    protected TSerializer initialValue() {
      try {
        return new TSerializer(FACTORY);
      } catch (TTransportException e) {
        throw new RuntimeException("Error creating serializer", e);
      }
    }
  };

  ThreadLocal<TDeserializer> DESERIALIZER = new ThreadLocal<>() {
    @Override
    protected TDeserializer initialValue() {
      try {
        return new TDeserializer(FACTORY);
      } catch (TTransportException e) {
        throw new RuntimeException("Error creating serializer", e);
      }
    }
  };

  default byte[] serialize(T object) throws TException {
    return SERIALIZER.get().serialize(object);
  }

  default void deserialize(T object, byte[] bytes) throws TException {
    DESERIALIZER.get().deserialize(null, bytes);
  }

}
