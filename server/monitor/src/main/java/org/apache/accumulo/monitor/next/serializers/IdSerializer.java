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

import org.apache.accumulo.server.metrics.MetricResponseWrapper;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.Tag;

public class IdSerializer extends JsonSerializer<Id> {

  @Override
  public void serialize(Id value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    StringBuilder buf = new StringBuilder();
    buf.append(value.getName());
    if (value.getTags() != null && value.getTags().size() > 0) {
      for (Tag t : value.getTags()) {
        if (t.getKey().endsWith(MetricResponseWrapper.STATISTIC_TAG)) {
          buf.append(".");
          buf.append(t.getValue());
        }
      }
    }

    gen.writeFieldName(buf.toString());
  }

}
