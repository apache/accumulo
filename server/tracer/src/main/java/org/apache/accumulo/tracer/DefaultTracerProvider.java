/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tracer;

import org.apache.accumulo.core.trace.OpenTelemetryFactory;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.autoconfigure.OpenTelemetrySdkAutoConfiguration;

@AutoService(OpenTelemetryFactory.class)
public class DefaultTracerProvider implements OpenTelemetryFactory {

  @Override
  public Tracer getOpenTelemetry() {
    // Set the service name if not set
    String svcNameEnvVar = System.getenv("OTEL_SERVICE_NAME");
    String svcNameProp = System.getenv("otel.service.name");
    String appName = System.getProperty("accumulo.application"); // set in accumulo-env.sh
    if (StringUtils.isEmpty(svcNameEnvVar) && StringUtils.isEmpty(svcNameProp)
        && !StringUtils.isEmpty(appName)) {
      System.setProperty("otel.service.name", appName);
    }
    // Configures a global OpenTelemetry object that can be configured using
    // the instructions at
    // https://github.com/open-telemetry/opentelemetry-java/tree/main/sdk-extensions/autoconfigure
    OpenTelemetry otel = OpenTelemetrySdkAutoConfiguration.initialize();
    return otel.getTracer(TraceUtil.INSTRUMENTATION_NAME);
  }

}
