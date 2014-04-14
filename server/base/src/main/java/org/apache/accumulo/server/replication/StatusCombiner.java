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
package org.apache.accumulo.server.replication;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.ValueFormatException;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.replication.proto.Replication.Status.Builder;
import org.apache.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Defines the rules for combining multiple {@link Status} messages
 * 
 * Messages that are "closed", stay closed. "Begin" and "end" always choose the maximum of the two.
 */
public class StatusCombiner extends TypedValueCombiner<Status> {

  public static class StatusEncoder implements Encoder<Status> {
    private static final Logger log = Logger.getLogger(StatusEncoder.class);

    @Override
    public byte[] encode(Status v) {
      return v.toByteArray();
    }

    @Override
    public Status decode(byte[] b) throws ValueFormatException {
      try {
        return Status.parseFrom(b);
      } catch (InvalidProtocolBufferException e) {
        log.error("Failed to parse Status protocol buffer", e);
        throw new ValueFormatException(e);
      }
    }
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("StatusCombiner");
    io.setDescription("Combiner that joins multiple Status protobufs to track replication metadata");
    return io;
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    return super.validateOptions(options);
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);

    setEncoder(new StatusEncoder());
  }

  @Override
  public Status typedReduce(Key key, Iterator<Status> iter) {
    Builder combined = null;

    while (iter.hasNext()) {
      Status status = iter.next();

      // Avoid creation of a new builder and message when we only have one
      // message to reduce
      if (null == combined) {
        if (!iter.hasNext()) {
          return status;
        } else {
          combined = Status.newBuilder();
        }
      }

      // Add the new message in with the previous message(s)
      combine(combined, status);
    }

    // Default case
    if (null == combined) {
      return Status.newBuilder().setBegin(0l).setEnd(0l).setClosed(true).build();
    }

    return combined.build();
  }

  /**
   * Update a {@link Builder} with another {@link Status}
   * 
   * @param combined
   *          The Builder to combine into
   * @param status
   *          The Status we're combining
   */
  public void combine(Builder combined, Status status) {
    // offset up to which replication is completed
    combined.setBegin(Math.max(combined.getBegin(), status.getBegin()));

    // offset up to which replication is necessary
    combined.setEnd(Math.max(combined.getEnd(), status.getEnd()));

    // will more data be added to the underlying file
    combined.setClosed(combined.getClosed() | status.getClosed());
  }
}
