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

import java.util.function.Supplier;

import org.apache.accumulo.core.compaction.thrift.TCompactionStatusUpdate;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionList;
import org.apache.accumulo.core.tabletserver.thrift.ActiveCompactionList;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionStats;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransportException;

public class ThriftSerializers {

  public static class ThriftSerializer<T extends TBase<?,?>> {

    private static final TBinaryProtocol.Factory PROTOCOL_FACTORY = new TBinaryProtocol.Factory();

    private static final ThreadLocal<TSerializer> SERIALIZER = new ThreadLocal<>() {
      @Override
      protected TSerializer initialValue() {
        try {
          return new TSerializer(PROTOCOL_FACTORY);
        } catch (TTransportException e) {
          throw new RuntimeException("Error creating serializer", e);
        }
      }
    };

    private static final ThreadLocal<TDeserializer> DESERIALIZER = new ThreadLocal<>() {
      @Override
      protected TDeserializer initialValue() {
        try {
          return new TDeserializer(PROTOCOL_FACTORY);
        } catch (TTransportException e) {
          throw new RuntimeException("Error creating serializer", e);
        }
      }
    };

    // prevent construction outside of this class
    private ThriftSerializer() {}

    public byte[] serialize(T object) throws TException {
      return SERIALIZER.get().serialize(object);
    }

    public void deserialize(T object, byte[] bytes) throws TException {
      DESERIALIZER.get().deserialize(object, bytes);
    }

  }

  public static final Supplier<
      ThriftSerializer<ActiveCompactionList>> EXTERNAL_COMPACTION_ACTIVE_COMPACTION_LIST =
          () -> new ThriftSerializer<ActiveCompactionList>();

  public static final Supplier<
      ThriftSerializer<TExternalCompactionJob>> EXTERNAL_COMPACTION_JOB_SERIALIZER =
          () -> new ThriftSerializer<TExternalCompactionJob>();

  public static final Supplier<ThriftSerializer<TExternalCompactionList>> EXTERNAL_COMPACTION_LIST =
      () -> new ThriftSerializer<TExternalCompactionList>();

  public static final Supplier<
      ThriftSerializer<TCompactionStatusUpdate>> EXTERNAL_COMPACTION_STATUS_SERIALIZER =
          () -> new ThriftSerializer<TCompactionStatusUpdate>();

  public static final Supplier<
      ThriftSerializer<TCompactionStats>> EXTERNAL_COMPACTION_STATS_SERIALIZER =
          () -> new ThriftSerializer<TCompactionStats>();

}
