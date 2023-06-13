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

import java.lang.reflect.Type;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.Objects;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * Gson adapter to handle serializing and deserializing byte arrays using Base64 encoding.
 */
public class ByteArrayToBase64TypeAdapter
    implements JsonSerializer<byte[]>, JsonDeserializer<byte[]> {

  private static final Decoder decoder = Base64.getUrlDecoder();
  private static final Encoder encoder = Base64.getUrlEncoder();

  @Override
  public byte[] deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
      throws JsonParseException {
    return decoder.decode(json.getAsString());
  }

  @Override
  public JsonElement serialize(byte[] src, Type typeOfSrc, JsonSerializationContext context) {
    return new JsonPrimitive(encoder.encodeToString(src));
  }

  /**
   * Creates a new Gson instance that registers {@link ByteArrayToBase64TypeAdapter} for handling
   * serializing/deserializing byte[] types as Base64 encoded
   *
   * @return Gson instance
   */
  public static Gson createBase64Gson() {
    return registerBase64TypeAdapter(new GsonBuilder()).create();
  }

  /**
   * Register {@link ByteArrayToBase64TypeAdapter} for handling byte[] types on an existing
   * GsonBuilder
   *
   * @param gsonBuilder existing GsonBuilder
   * @return GsonBuilder
   */
  public static GsonBuilder registerBase64TypeAdapter(final GsonBuilder gsonBuilder) {
    return Objects.requireNonNull(gsonBuilder).registerTypeHierarchyAdapter(byte[].class,
        new ByteArrayToBase64TypeAdapter());
  }
}
