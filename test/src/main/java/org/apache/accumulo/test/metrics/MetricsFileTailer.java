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
package org.apache.accumulo.test.metrics;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.URL;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This class allows testing of the publishing to the hadoop metrics system by processing a file for
 * metric records (written as a line.) The file should be configured using the hadoop metrics
 * properties as a file based sink with the prefix that is provided on instantiation of the
 * instance.
 *
 * This class will simulate tail-ing a file and is intended to be run in a separate thread. When the
 * underlying file has data written, the vaule returned by getLastUpdate will change, and the last
 * line can be retrieved with getLast().
 */
public class MetricsFileTailer implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(MetricsFileTailer.class);

  private static final int BUFFER_SIZE = 4;

  private final String metricsPrefix;

  private final Lock lock = new ReentrantLock();
  private final AtomicBoolean running = new AtomicBoolean(Boolean.FALSE);

  private AtomicLong lastUpdate = new AtomicLong(0);
  private long startTime = System.nanoTime();

  private int lineCounter = 0;
  private String[] lineBuffer = new String[BUFFER_SIZE];

  private final String metricsFilename;

  /**
   * Create an instance that will tail a metrics file. The filename / path is determined by the
   * hadoop-metrics-accumulo.properties sink configuration for the metrics prefix that is provided.
   *
   * @param metricsPrefix
   *          the prefix in the metrics configuration.
   */
  public MetricsFileTailer(final String metricsPrefix) {

    this.metricsPrefix = metricsPrefix;

    Configuration sub = loadMetricsConfig();

    // dump received configuration keys received.
    if (log.isTraceEnabled()) {
      Iterator<String> keys = sub.getKeys();
      while (keys.hasNext()) {
        log.trace("configuration key:{}", keys.next());
      }
    }

    if (sub.containsKey("filename")) {
      metricsFilename = sub.getString("filename");
    } else {
      metricsFilename = "";
    }

  }

  /**
   * Create an instance by specifying a file directly instead of using the metrics configuration -
   * mainly for testing.
   *
   * @param metricsPrefix
   *          generally can be ignored.
   * @param filename
   *          the path / file to be monitored.
   */
  MetricsFileTailer(final String metricsPrefix, final String filename) {
    this.metricsPrefix = metricsPrefix;
    metricsFilename = filename;
  }

  /**
   * Look for the accumulo metrics configuration file on the classpath and return the subset for the
   * http sink.
   *
   * @return a configuration with http sink properties.
   */
  @SuppressFBWarnings(value = "URLCONNECTION_SSRF_FD",
      justification = "url specified by test code, not unchecked user input")
  private Configuration loadMetricsConfig() {
    final URL propUrl =
        getClass().getClassLoader().getResource(MetricsTestSinkProperties.METRICS_PROP_FILENAME);

    if (propUrl == null) {
      throw new IllegalStateException(
          "Could not find " + MetricsTestSinkProperties.METRICS_PROP_FILENAME + " on classpath");
    }

    // Read data from this file
    var config = new PropertiesConfiguration();
    try (var reader = new InputStreamReader(propUrl.openStream(), UTF_8)) {
      config.read(reader);
    } catch (ConfigurationException | IOException e) {
      throw new IllegalStateException(
          String.format("Could not find configuration file \'%s\' on classpath",
              MetricsTestSinkProperties.METRICS_PROP_FILENAME));
    }

    final Configuration sub = config.subset(metricsPrefix);

    if (log.isTraceEnabled()) {
      log.trace("Config {}", config);
      Iterator<String> iterator = sub.getKeys();
      while (iterator.hasNext()) {
        String key = iterator.next();
        log.trace("'{}'='{}'", key, sub.getProperty(key));
      }
    }

    return sub;
  }

  /**
   * Creates a marker value that increases each time a new line is detected. Clients can use this to
   * determine if a call to getLast() will return a new value. However, this value is <b>NOT</b> a
   * timestamp and should not be interpreted as such. Furthermore, it does not indicate that the
   * metrics being reported have changed, only that a new metrics poll took place and was written to
   * the file. So, if clients need to observe new metrics from a new event, they need to parse the
   * line themselves to look for changed metrics values.
   *
   * @return a marker value set when a new line is available.
   */
  public long getLastUpdate() {
    return lastUpdate.get();
  }

  /**
   * Get the last line seen in the file.
   *
   * @return the last line from the file.
   */
  public String getLast() {
    lock.lock();
    try {

      int last = (lineCounter % BUFFER_SIZE) - 1;
      if (last < 0) {
        last = BUFFER_SIZE - 1;
      }
      return lineBuffer[last];
    } finally {
      lock.unlock();
    }
  }

  /**
   * The hadoop metrics file sink published records as a line with comma separated key=value pairs.
   * This method parses the line and extracts the key, value pair from metrics that start with an
   * optional prefix and returns them in a sort map. If the prefix is null or empty, all keys are
   * accepted.
   *
   * @param prefix
   *          optional filter - include metrics that start with provided value..
   * @return a map of the metrics that start with AccGc
   */
  public Map<String,String> parseLine(final String prefix) {

    String line = getLast();

    if (line == null) {
      return Collections.emptyMap();
    }

    Map<String,String> m = new TreeMap<>();

    String[] csvTokens = line.split(",");

    for (String token : csvTokens) {
      token = token.trim();
      if (filter(prefix, token)) {
        String[] parts = token.split("=");
        m.put(parts[0], parts[1]);
      }
    }
    return m;
  }

  private boolean filter(final String prefix, final String candidate) {
    if (candidate == null) {
      return false;
    }

    if (prefix == null || prefix.isEmpty()) {
      return true;
    }
    return candidate.startsWith(prefix);
  }

  /**
   * A loop that polls for changes and when the file changes, put the last line in a buffer that can
   * be retrieved by clients using getLast().
   */
  private void run() {

    long filePos = 0;

    File f = new File(metricsFilename);

    while (running.get()) {

      try {
        Thread.sleep(5_000);
      } catch (InterruptedException ex) {
        running.set(Boolean.FALSE);
        Thread.currentThread().interrupt();
        return;
      }

      long len = f.length();

      try {

        // file truncated? reset position
        if (len < filePos) {
          filePos = 0;
          lock.lock();
          try {
            for (int i = 0; i < BUFFER_SIZE; i++) {
              lineBuffer[i] = "";
            }
            lineCounter = 0;
          } finally {
            lock.unlock();
          }
        }

        if (len > filePos) {
          // File must have had something added to it!
          RandomAccessFile raf = new RandomAccessFile(f, "r");
          raf.seek(filePos);
          String line;
          lock.lock();
          try {
            while ((line = raf.readLine()) != null) {
              lineBuffer[lineCounter++ % BUFFER_SIZE] = line;
            }

            lastUpdate.set(System.nanoTime() - startTime);

          } finally {
            lock.unlock();
          }
          filePos = raf.getFilePointer();
          raf.close();
        }
      } catch (Exception ex) {
        log.info("Error processing metrics file {}", metricsFilename, ex);
      }
    }
  }

  public void startDaemonThread() {
    if (running.compareAndSet(false, true)) {
      Thread t = new Thread(() -> this.run());
      t.setDaemon(true);
      t.start();
    }
  }

  @Override
  public void close() {
    running.set(Boolean.FALSE);
  }

  // utilities to block, waiting for update - call from process thread

  public static class LineUpdate {
    private final long lastUpdate;
    private final String line;

    public LineUpdate(long lastUpdate, String line) {
      this.lastUpdate = lastUpdate;
      this.line = line;
    }

    public long getLastUpdate() {
      return lastUpdate;
    }

    public String getLine() {
      return line;
    }

    @Override
    public String toString() {
      return "LineUpdate{" + "lastUpdate=" + lastUpdate + ", line='" + line + '\'' + '}';
    }
  }

  public LineUpdate waitForUpdate(final long prevUpdate, final int maxAttempts, final long delay) {

    for (int count = 0; count < maxAttempts; count++) {

      String line = getLast();
      long currUpdate = getLastUpdate();

      if (line != null && (currUpdate != prevUpdate)) {
        return new LineUpdate(getLastUpdate(), line);
      }

      try {
        Thread.sleep(delay);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(ex);
      }
    }
    // not found - throw exception.
    throw new IllegalStateException(
        String.format("File source update not received after %d tries in %d sec", maxAttempts,
            TimeUnit.MILLISECONDS.toSeconds(delay * maxAttempts)));
  }

}
