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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/**
 * throw away test class for development.
 */
public class MetricsClientTest {

  private static final Logger log = LoggerFactory.getLogger(MetricsClientTest.class);

  AtomicReference<String> payload = new AtomicReference<>(null);

  private HttpServer server;

  @Before
  public void init() throws Exception {

    String context = "post";

    server = HttpServer.create(new InetSocketAddress(InetAddress.getLocalHost(), 12332), 0);
    server.createContext("/" + context, new MyHandler(payload));
    server.setExecutor(null); // creates a default executor
    server.start();

  }

  static class MyHandler implements HttpHandler {

    private AtomicReference<String> update;

    public MyHandler(AtomicReference<String> update) {
      this.update = update;
    }

    @Override
    public void handle(HttpExchange t) throws IOException {
      log.info("Received H: {}", t.getRequestHeaders().entrySet());
      log.info("Received M: {}", t.getRequestMethod());

      log.info("C: {}", t.getHttpContext());

      InputStream is = t.getRequestBody();
      log.info("MB:{}", IOUtils.toString(is));

      t.sendResponseHeaders(200, -1);
      // OutputStream os = t.getResponseBody();
      // os.write("200".getBytes(StandardCharsets.UTF_8));
      // os.close();
    }
  }

  @Test
  public void post() throws Exception {

    CloseableHttpClient httpclient = HttpClients.createDefault();

    final String u =
        String.format("http://%s:%d/post", InetAddress.getLocalHost().getHostAddress(), 12332);

    log.info("Connect to: {}", u);

    try {

      JsonMetricsValues v = new JsonMetricsValues();
      v.setTimestamp(System.currentTimeMillis());
      v.addMetric("a", "1");
      v.addMetric("b", "2");
      v.sign();

      StringEntity entity = new StringEntity(v.toJson());
      HttpPost httpPost = new HttpPost(u);

      httpPost.setEntity(entity);
      httpPost.setHeader("Accept", "application/json");
      httpPost.setHeader("Content-type", "application/json");

      CloseableHttpResponse response = httpclient.execute(httpPost);

      log.info("PR: {}", response.getStatusLine().getStatusCode());

      httpclient.close();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }
}
