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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JsonMetricsValues {

  private static final Logger log = LoggerFactory.getLogger(JsonMetricsValues.class);

  private long timestamp;
  private final Map<String,String> metrics;
  private String signature;

  public JsonMetricsValues() {
    metrics = new TreeMap<>();
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public Map<String,String> getMetrics() {
    return metrics;
  }

  public void addMetric(final String name, final String value) {
    metrics.put(name, value);
  }

  public String getSignature() {
    return signature;
  }

  /**
   * Similar to git sha-1, use 8 bytes of a message digest to create a hex formatted string to
   * create a signature for the contents.
   */
  public void sign() {
    byte[] md = digest();
    signature = Long.toHexString(mdformatter(md));
  }

  /**
   * Verify that the candidate signatues matches the digest calculated from the current contents.
   *
   * @param candidate
   *          a hex formatted, 8-byte signature
   *
   * @return true if content signature matches provided candidate.
   */
  public boolean verify(final String candidate) {
    byte[] md = digest();
    String calculated = Long.toHexString(mdformatter(md));
    return calculated.equals(candidate);
  }

  private byte[] digest() {
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException ex) {
      byte[] invalid = {-1};
      return invalid;
    }

    digest.reset();
    digest.update(Long.toString(timestamp).getBytes());
    for (Map.Entry<String,String> e : metrics.entrySet()) {
      digest.update(e.getKey().getBytes());
      digest.update(e.getValue().getBytes());
    }
    return digest.digest();
  }

  /**
   * Create a long from the first 8 bytes to create a signature.
   *
   * @param bytes
   *          byte array
   * @return hex-formatted string of first 8 bytes.
   */
  private Long mdformatter(final byte[] bytes) {

    long x = 0;
    for (int i = 0; i < 8 && i < bytes.length; i++) {
      x |= (0xff & (long) bytes[i]) << i * 8;
    }
    return x;
  }

  public String toJson() {
    Gson gson = new GsonBuilder().create();
    return gson.toJson(this);
  }

  public static JsonMetricsValues fromJson(final String json) {
    Gson gson = new GsonBuilder().create();
    return gson.fromJson(json, JsonMetricsValues.class);
  }

  public static JsonMetricsValues fromJson(final InputStream in) {

    try {
      Gson gson = new GsonBuilder().create();
      return gson.fromJson(IOUtils.toString(in), JsonMetricsValues.class);
    } catch (IOException ex) {
      log.info("Failed to process input stream");
    }
    return null;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("JsonValues{");
    sb.append("timestamp=").append(timestamp);
    sb.append(", metrics=").append(metrics);
    sb.append(", signature='").append(signature).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
