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
package org.apache.accumulo.server.metrics;

import static com.google.common.base.Charsets.UTF_8;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.lang.time.DateUtils;

public abstract class AbstractMetricsImpl {

  public class Metric {

    private long count = 0;
    private long avg = 0;
    private long min = 0;
    private long max = 0;

    public long getCount() {
      return count;
    }

    public long getAvg() {
      return avg;
    }

    public long getMin() {
      return min;
    }

    public long getMax() {
      return max;
    }

    public void incCount() {
      count++;
    }

    public void addAvg(long a) {
      if (a < 0)
        return;
      avg = (long) ((avg * .8) + (a * .2));
    }

    public void addMin(long a) {
      if (a < 0)
        return;
      min = Math.min(min, a);
    }

    public void addMax(long a) {
      if (a < 0)
        return;
      max = Math.max(max, a);
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("count", count).append("average", avg).append("minimum", min)
          .append("maximum", max).toString();
    }

  }

  static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(AbstractMetricsImpl.class);

  private static ConcurrentHashMap<String,Metric> registry = new ConcurrentHashMap<String,Metric>();

  private boolean currentlyLogging = false;

  private File logDir = null;

  private String metricsPrefix = null;

  private Date today = new Date();

  private File logFile = null;

  private Writer logWriter = null;

  private SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");

  private SimpleDateFormat logFormatter = new SimpleDateFormat("yyyyMMddhhmmssz");

  private MetricsConfiguration config = null;

  public AbstractMetricsImpl() {
    this.metricsPrefix = getMetricsPrefix();
    config = new MetricsConfiguration(metricsPrefix);
  }

  /**
   * Registers a StandardMBean with the MBean Server
   */
  public void register(StandardMBean mbean) throws Exception {
    // Register this object with the MBeanServer
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    if (null == getObjectName())
      throw new IllegalArgumentException("MBean object name must be set.");
    mbs.registerMBean(mbean, getObjectName());

    setupLogging();
  }

  /**
   * Registers this MBean with the MBean Server
   */
  public void register() throws Exception {
    // Register this object with the MBeanServer
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    if (null == getObjectName())
      throw new IllegalArgumentException("MBean object name must be set.");
    mbs.registerMBean(this, getObjectName());
    setupLogging();
  }

  public void createMetric(String name) {
    registry.put(name, new Metric());
  }

  public Metric getMetric(String name) {
    return registry.get(name);
  }

  public long getMetricCount(String name) {
    return registry.get(name).getCount();
  }

  public long getMetricAvg(String name) {
    return registry.get(name).getAvg();
  }

  public long getMetricMin(String name) {
    return registry.get(name).getMin();
  }

  public long getMetricMax(String name) {
    return registry.get(name).getMax();
  }

  private void setupLogging() throws IOException {
    if (null == config.getMetricsConfiguration())
      return;
    // If we are already logging, then return
    if (!currentlyLogging && config.getMetricsConfiguration().getBoolean(metricsPrefix + ".logging", false)) {
      // Check to see if directory exists, else make it
      String mDir = config.getMetricsConfiguration().getString("logging.dir");
      if (null != mDir) {
        File dir = new File(mDir);
        if (!dir.isDirectory())
          if (!dir.mkdir())
            log.warn("Could not create log directory: " + dir);
        logDir = dir;
        // Create new log file
        startNewLog();
      }
      currentlyLogging = true;
    }
  }

  private void startNewLog() throws IOException {
    if (null != logWriter) {
      logWriter.flush();
      logWriter.close();
    }
    logFile = new File(logDir, metricsPrefix + "-" + formatter.format(today) + ".log");
    if (!logFile.exists()) {
      if (!logFile.createNewFile()) {
        log.error("Unable to create new log file");
        currentlyLogging = false;
        return;
      }
    }
    logWriter = new OutputStreamWriter(new FileOutputStream(logFile, true), UTF_8);
  }

  private void writeToLog(String name) throws IOException {
    if (null == logWriter)
      return;
    // Increment the date if we have to
    Date now = new Date();
    if (!DateUtils.isSameDay(today, now)) {
      today = now;
      startNewLog();
    }
    logWriter.append(logFormatter.format(now)).append(" Metric: ").append(name).append(": ").append(registry.get(name).toString()).append("\n");
  }

  public void add(String name, long time) {
    if (isEnabled()) {
      registry.get(name).incCount();
      registry.get(name).addAvg(time);
      registry.get(name).addMin(time);
      registry.get(name).addMax(time);
      // If we are not currently logging and should be, then initialize
      if (!currentlyLogging && config.getMetricsConfiguration().getBoolean(metricsPrefix + ".logging", false)) {
        try {
          setupLogging();
        } catch (IOException ioe) {
          log.error("Error setting up log", ioe);
        }
      } else if (currentlyLogging && !config.getMetricsConfiguration().getBoolean(metricsPrefix + ".logging", false)) {
        // if we are currently logging and shouldn't be, then close logs
        try {
          logWriter.flush();
          logWriter.close();
          logWriter = null;
          logFile = null;
        } catch (Exception e) {
          log.error("Error stopping metrics logging", e);
        }
        currentlyLogging = false;
      }
      if (currentlyLogging) {
        try {
          writeToLog(name);
        } catch (IOException ioe) {
          log.error("Error writing to metrics log", ioe);
        }
      }
    }
  }

  public boolean isEnabled() {
    return config.isEnabled();
  }

  protected abstract ObjectName getObjectName();

  protected abstract String getMetricsPrefix();

  @Override
  protected void finalize() {
    if (null != logWriter) {
      try {
        logWriter.close();
      } catch (Exception e) {
        // do nothing
      } finally {
        logWriter = null;
      }
    }
    logFile = null;
  }

}
