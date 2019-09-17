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

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class HttpMetrics2Receiver {

  private static final Logger log = LoggerFactory.getLogger(HttpMetrics2Receiver.class);

  // set max content length for messages to 1M
  private static final int MAX_MESSAGE_PAYLOAD = 1024 * 1024;

  private final HttpServer server;
  private final AtomicReference<JsonMetricsValues> metrics = new AtomicReference<>();

  private final String metricsPrefix;

  /**
   * Looks on the classpath for hadoop-metrics2-accumulo.properties file and configures an http
   * server that will listen for POST requests from the HttpMetrics sink. Multiple instances can be
   * created if the use different prefixes which defines separate ports. This class is to help
   * testing metrics and has not been developed or tested as a generic http metrics listener for
   * production.
   *
   * @param metricsPrefix
   *          defines the subset of properties that will be used for configuration.
   * @throws IOException
   *           if the http server cannot be created.
   */
  public HttpMetrics2Receiver(final String metricsPrefix) throws IOException {

    this.metricsPrefix = metricsPrefix;

    Configuration sub = loadMetricsConfig();

    String host = sub.getString(HttpMetrics2SinkProperties.HTTP_HOST_PROP_NAME);
    int port = sub.getInt(HttpMetrics2SinkProperties.HTTP_PORT_PROP_NAME);

    // strip starting /'s if any.
    String endpoint =
        sub.getString(HttpMetrics2SinkProperties.HTTP_ENDPOINT_PROP_NAME).replaceAll("^/+", "");

    log.info("Creating http server that will listen on {}:{}/{} for POST", host, port, endpoint);

    server = HttpServer.create(new InetSocketAddress(host, port), 0);
    server.createContext("/" + endpoint, new MyHandler(metrics));
    server.setExecutor(null); // creates a default executor
    server.start();

  }

  /**
   * Look for the accumulo metrics configuration file on the classpath and return the subset for the
   * http sink.
   *
   * @return a configuration with http sink properties.
   */
  private Configuration loadMetricsConfig() {
    try {

      final URL propUrl =
          getClass().getClassLoader().getResource(HttpMetrics2SinkProperties.METRICS_PROP_FILENAME);

      if (propUrl == null) {
        throw new IllegalStateException(
            "Could not find " + HttpMetrics2SinkProperties.METRICS_PROP_FILENAME + " on classpath");
      }

      String filename = propUrl.getFile();
      Configuration config = new PropertiesConfiguration(filename);

      final Configuration sub = config.subset(metricsPrefix);

      if (log.isTraceEnabled()) {
        log.trace("Config {}", config);
        @SuppressWarnings("unchecked")
        Iterator<String> iterator = sub.getKeys();
        while (iterator.hasNext()) {
          String key = iterator.next();
          log.trace("'{}\'=\'{}\'", key, sub.getProperty(key));
        }
      }

      return sub;

    } catch (ConfigurationException ex) {
      throw new IllegalStateException(
          String.format("Could not find configuration file \'%s\' on classpath",
              HttpMetrics2SinkProperties.METRICS_PROP_FILENAME));
    }
  }

  public JsonMetricsValues getMetrics() {
    return metrics.get();
  }

  static class MyHandler implements HttpHandler {

    private final AtomicReference<JsonMetricsValues> update;

    MyHandler(AtomicReference<JsonMetricsValues> update) {
      this.update = update;
    }

    @Override
    public void handle(HttpExchange t) {

      if (log.isTraceEnabled()) {
        // print received message header
        log.trace("Received Request Header: {}", t.getRequestHeaders().entrySet());
        log.trace("Request Method: {}", t.getRequestMethod());
        log.trace("HTTP context: {}", t.getHttpContext().getAttributes());
      }

      // guard excessive length malformed messages

      String headerValue = "";
      try {
        headerValue = t.getRequestHeaders().getFirst("Content-length");
        int len = Integer.parseInt(headerValue);
        if (len > MAX_MESSAGE_PAYLOAD) {
          throw new IllegalStateException(
              "Message content length " + len + " exceeded max allowed " + MAX_MESSAGE_PAYLOAD);
        }

      } catch (NumberFormatException nex) {
        throw new IllegalArgumentException("Invalid request header length " + headerValue, nex);
      }

      if (t.getRequestMethod().equals("POST")) {
        processPost(t);
      }

    }

    private void processPost(final HttpExchange t) {

      try (InputStream is = t.getRequestBody()) {

        String msgContents = IOUtils.toString(is);
        is.close();

        update.set(JsonMetricsValues.fromJson(msgContents));

        t.sendResponseHeaders(200, -1);

      } catch (IOException ex) {
        log.info("failed to process POST message");
      }
    }
  }
}
