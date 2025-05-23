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
package org.apache.accumulo.monitor.next.serializers;

import java.io.IOException;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class ThriftSerializer extends JsonSerializer<TBase<?,?>> {

  private static final Logger LOG = LoggerFactory.getLogger(ThriftSerializer.class);
  private final TSimpleJSONProtocol.Factory factory = new TSimpleJSONProtocol.Factory();

  @Override
  public void serialize(TBase<?,?> value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    try {
      // TSerializer is likely not thread safe
      gen.writeRaw(new TSerializer(factory).toString(value));
    } catch (TException e) {
      LOG.error("Error serializing Thrift object", e);
    }
  }

}
