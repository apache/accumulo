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

import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.metrics.flatbuffers.FTag;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class FMetricSerializer extends JsonSerializer<FMetric> {

  @Override
  public void serialize(FMetric value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    if (value == null) {
      return;
    }
    FMetric fm = FMetric.getRootAsFMetric(value.getByteBuffer());
    gen.writeStartObject();
    gen.writeStringField("name", fm.name());
    gen.writeStringField("type", fm.type());
    gen.writeArrayFieldStart("tags");
    for (int i = 0; i < fm.tagsLength(); i++) {
      FTag t = fm.tags(i);
      gen.writeStartObject();
      gen.writeStringField(t.key(), t.value());
      gen.writeEndObject();
    }
    gen.writeEndArray();
    // Write the non-zero number as the value
    if (fm.lvalue() > 0) {
      gen.writeNumberField("value", fm.lvalue());
    } else if (fm.ivalue() > 0) {
      gen.writeNumberField("value", fm.ivalue());
    } else if (fm.dvalue() > 0.0d) {
      gen.writeNumberField("value", fm.dvalue());
    } else {
      gen.writeNumberField("value", 0);
    }
    gen.writeEndObject();

  }

}
