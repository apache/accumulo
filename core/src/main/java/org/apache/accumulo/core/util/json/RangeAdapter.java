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
package org.apache.accumulo.core.util.json;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Type;
import java.util.Objects;

import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * Gson adapter that can serialize {@link Range} objects to Base64 encoding.
 *
 * Ranges will first be serialized to a byte array and then to Base64. Null ranges will be
 * serialized as a byte array of size 0.
 */
public class RangeAdapter implements JsonSerializer<Range>, JsonDeserializer<Range> {

  private static final ByteArrayToBase64TypeAdapter BASE64_SERIALIZER =
      new ByteArrayToBase64TypeAdapter();

  @Override
  public Range deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
      throws JsonParseException {
    return decodeRange(BASE64_SERIALIZER.deserialize(json, typeOfT, context));
  }

  @Override
  public JsonElement serialize(Range src, Type typeOfSrc, JsonSerializationContext context) {
    return BASE64_SERIALIZER.serialize(encodeRange(src), typeOfSrc, context);
  }

  /**
   * Helper methods to encode and decode a range to/from byte arrays. Note that the Gson serializer
   * won't pass in null values as they are handled separately
   */

  private static byte[] encodeRange(final Range range) {
    try {
      try (DataOutputBuffer buffer = new DataOutputBuffer()) {
        range.write(buffer);
        return buffer.getData();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Range decodeRange(byte[] serialized) {
    try {
      try (DataInputBuffer buffer = new DataInputBuffer()) {
        final Range range = new Range();
        buffer.reset(serialized, serialized.length);
        range.readFields(buffer);
        return range;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Creates a new Gson instance that registers a {@link RangeAdapter} for handling
   * serializing/deserializing byte[] types as Base64 encoded
   *
   * @return Gson instance
   */
  public static Gson createRangeGson() {
    return registerRangeAdapter(new GsonBuilder()).create();
  }

  /**
   * Register {@link RangeAdapter} for handling {@link Range} types on an existing GsonBuilder
   *
   * @param gsonBuilder existing GsonBuilder
   * @return GsonBuilder
   */
  public static GsonBuilder registerRangeAdapter(final GsonBuilder gsonBuilder) {
    return Objects.requireNonNull(gsonBuilder).registerTypeHierarchyAdapter(Range.class,
        new RangeAdapter());
  }
}
