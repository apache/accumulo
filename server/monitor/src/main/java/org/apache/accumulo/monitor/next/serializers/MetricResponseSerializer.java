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
import java.nio.ByteBuffer;

import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.metrics.flatbuffers.FTag;
import org.apache.accumulo.core.metrics.thrift.MetricResponse;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class MetricResponseSerializer extends JsonSerializer<MetricResponse> {

  @Override
  public void serialize(MetricResponse value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    gen.writeStartObject();
    gen.writeNumberField("timestamp", value.getTimestamp());
    gen.writeStringField("serverType", value.getServerType().toString());
    gen.writeStringField("resourceGroup", value.getResourceGroup());
    gen.writeStringField("host", value.getServer());
    gen.writeArrayFieldStart("metrics");
    if (value.getMetrics() != null) {
      for (final ByteBuffer binary : value.getMetrics()) {
        FMetric fm = FMetric.getRootAsFMetric(binary);
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
      gen.writeEndArray();
      gen.writeEndObject();
    }
  }

}
