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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.server.rest.ThriftSerializer.ENCODED;
import static org.apache.accumulo.server.rest.ThriftSerializer.TYPE;

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Jackson deserializer for thrift objects that delegates the deserialization of thrift to
 * {@link TDeserializer}. It handles previously encoded serialized objects from
 * {@link ThriftSerializer}
 */
public class ThriftDeserializer<T extends TBase<?,?>> extends JsonDeserializer<T> {
  @Override
  public T deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    JsonNode tree = p.readValueAsTree();

    try {
      var thriftClassName = tree.get(TYPE).asText();
      var encoded = tree.get(ENCODED).asText();

      Constructor<T> constructor = getThriftClass(thriftClassName).getDeclaredConstructor();
      T obj = constructor.newInstance();
      deserialize(obj, encoded);

      return obj;
    } catch (ReflectiveOperationException e) {
      throw new IOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private Class<T> getThriftClass(String className) throws ClassNotFoundException {
    var clazz = Class.forName(className, false, ThriftDeserializer.class.getClassLoader());
    // Note: This check is important to prevent potential security issues
    // We don't want to allow arbitrary classes to be loaded
    if (!TBase.class.isAssignableFrom(clazz)) {
      throw new IllegalArgumentException("Class " + clazz + " is not assignable to TBase");
    }
    return (Class<T>) clazz;
  }

  // TODO: It doesn't seem like TDeserializer is thread safe, is there a way
  // to prevent creating a new deserializer for every object?
  private static <T extends TBase<?,?>> void deserialize(T obj, String json) throws IOException {
    try {
      final TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());
      deserializer.deserialize(obj, json, UTF_8.name());
    } catch (TException e) {
      throw new IOException(e);
    }
  }
}
