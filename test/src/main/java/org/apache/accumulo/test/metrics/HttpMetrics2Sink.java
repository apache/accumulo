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

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A hadoop metrics sink that pushes results via http using POST. The sink can also be configured to
 * dump the same metrics to a file for debugging.
 */
public class HttpMetrics2Sink implements MetricsSink, Closeable {

  private static final Logger log = LoggerFactory.getLogger(HttpMetrics2Sink.class);

  private final AtomicBoolean initialized = new AtomicBoolean(Boolean.FALSE);

  private PrintWriter writer = null;
  private String postUrl = "";

  @Override
  public void putMetrics(MetricsRecord metricsRecord) {

    JsonMetricsValues metrics = new JsonMetricsValues();
    metrics.setTimestamp(System.currentTimeMillis());

    for (AbstractMetric r : metricsRecord.metrics()) {
      metrics.addMetric(r.name(), Long.toString(r.value().longValue()));
    }

    metrics.sign();

    String json = metrics.toJson();

    httpPost(json);

    // stub.set(lastUpdate.get().toJson());

    writer.println(String.format("%s", json));
    writer.flush();
  }

  @Override
  public void flush() {
    if (writer != null) {
      writer.flush();
    }
  }

  public synchronized void close() {
    if (writer != null) {
      writer.flush();
      PrintWriter tmp = writer;
      writer = null;
      tmp.close();
    }
  }

  @Override
  public synchronized void init(SubsetConfiguration config) {

    if (initialized.get()) {
      return;
    }

    try {

      if (config.containsKey(HttpMetrics2SinkProperties.OUT_FILENAME_PROP_NAME)) {
        createOutputFile(config);
      }

      if (config.containsKey(HttpMetrics2SinkProperties.HTTP_HOST_PROP_NAME)) {

        String host = config.getString(HttpMetrics2SinkProperties.HTTP_HOST_PROP_NAME, "");
        int port = config.getInt(HttpMetrics2SinkProperties.HTTP_PORT_PROP_NAME);
        String endpoint = config.getString(HttpMetrics2SinkProperties.HTTP_ENDPOINT_PROP_NAME, "");

        endpoint = endpoint.replaceAll("^/+", "");

        postUrl = String.format("http://%s:%d/%s", host, port, endpoint);

      }
    } catch (Exception ex) {
      log.error("Configuration error prevented initialization - sink may not be available", ex);
      return;
    }

    initialized.set(Boolean.TRUE);

  }

  /**
   * Sink output will be logged to a file if a file / pathname is provided in the sink
   * configuration. The file can be optionally truncated.
   *
   * @param config
   *          sink configuration.
   */
  private void createOutputFile(SubsetConfiguration config) {

    // prevent multiple file initialization calls when init fails.
    if (writer != null) {
      return;
    }

    // use context to avoid file name collisions.
    String context = "";
    if (config.containsKey("context")) {
      context = String.format("%s_", config.getProperty("context"));
    }

    String filename = config.getString(HttpMetrics2SinkProperties.OUT_FILENAME_PROP_NAME,
        "/tmp/" + context + "metrics.txt");

    try {

      boolean truncate =
          config.getBoolean(HttpMetrics2SinkProperties.TRUNCATE_FILE_PROP_NAME, Boolean.FALSE);

      // this erases previous content
      if (truncate) {
        FileWriter dummy = new FileWriter(filename);
        dummy.close();
      }

      // this reopens file for appending
      writer = new PrintWriter(new BufferedWriter(new FileWriter(filename, true)));

      if (config.getBoolean(HttpMetrics2SinkProperties.DUMP_CONFIG_PROP_NAME, Boolean.FALSE)) {
        dumpConfig(config);
      }

    } catch (IOException ex) {
      ex.printStackTrace();
      if (writer != null) {
        writer.close();
        writer = null;
      }
    }
  }

  /**
   * Write the received configuration to the output file if it is configured and dump requested.
   *
   * @param config
   *          The subset configuration received from metrics system.
   */
  private void dumpConfig(SubsetConfiguration config) {

    if (writer == null) {
      return;
    }

    @SuppressWarnings("unchecked")
    Iterator<String> keys = config.getKeys();
    writer.println("::config start::");
    while (keys.hasNext()) {
      String key = keys.next();
      writer.println(String.format("'%s\'=\'%s\'", key, config.getProperty(key)));
    }
    writer.println("::config end::");

  }

  /**
   * POST the json metrics object.
   *
   * @param json
   *          json formatted metrics values, as string.
   */
  private synchronized void httpPost(final String json) {

    try (CloseableHttpClient httpclient = HttpClients.createDefault()) {

      final String postUri = postUrl;

      log.debug("post destination uri: \'{}\'", postUri);

      StringEntity entity = new StringEntity(json);
      HttpPost httpPost = new HttpPost(postUri);

      httpPost.setEntity(entity);
      httpPost.setHeader("Accept", "application/json");
      httpPost.setHeader("Content-type", "application/json");

      CloseableHttpResponse response = httpclient.execute(httpPost);

      int status = response.getStatusLine().getStatusCode();

      if (status >= 200 && status < 300) {
        log.debug("metrics post successful");
      } else {
        log.debug("metrics post failed with status {}", status);
      }

    } catch (Exception ex) {
      log.debug("metrics POST failed, listener endpoint may not present", ex);
    }
  }
}
