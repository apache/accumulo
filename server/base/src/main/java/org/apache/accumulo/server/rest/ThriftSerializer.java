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
package org.apache.accumulo.server.rest;

import java.io.IOException;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * Jackson serializer for thrift objects that delegates the serialization of thrift to
 * {@link TSerializer} and also includes the serialized type for the deserializer to use
 */
public class ThriftSerializer<T extends TBase<?,?>> extends JsonSerializer<T> {

  static final String TYPE = "type";
  static final String ENCODED = "encoded";

  @Override
  public void serialize(T value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    gen.writeStartObject();
    gen.writeObjectField(TYPE, value.getClass());
    gen.writeStringField(ENCODED, serialize(value));
    gen.writeEndObject();
  }

  // TODO: It doesn't seem like TSerializer is thread safe, is there a way
  // to prevent creating a new serializer for every object?
  private static <T extends TBase<?,?>> String serialize(T obj) throws IOException {
    try {
      final TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
      return serializer.toString(obj);
    } catch (TException e) {
      throw new IOException(e);
    }
  }
}
