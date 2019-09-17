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
package org.apache.accumulo.test.metrics;

public class HttpMetrics2SinkProperties {

  public static final String METRICS_PROP_FILENAME = "hadoop-metrics2-accumulo.properties";
  public static final String ACC_GC_SINK_PREFIX = "accumulo.sink.http-gc";
  public static final String ACC_MASTER_SINK_PREFIX = "accumulo.sink.http-master";

  public static final String OUT_FILENAME_PROP_NAME = "filename";
  public static final String TRUNCATE_FILE_PROP_NAME = "truncate";
  public static final String DUMP_CONFIG_PROP_NAME = "dumpConfig";
  public static final String HTTP_HOST_PROP_NAME = "httpHostname";
  public static final String HTTP_PORT_PROP_NAME = "httpPort";
  public static final String HTTP_ENDPOINT_PROP_NAME = "httpPostEndpoint";

}
