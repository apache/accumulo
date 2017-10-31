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
package org.apache.accumulo.core.trace;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.SpanReceiverBuilder;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to enable tracing for Accumulo server processes.
 *
 */
public class DistributedTrace {
  private static final Logger log = LoggerFactory.getLogger(DistributedTrace.class);

  private static final String HTRACE_CONF_PREFIX = "hadoop.";

  public static final String TRACE_HOST_PROPERTY = "trace.host";
  public static final String TRACE_SERVICE_PROPERTY = "trace.service";

  public static final String TRACER_ZK_HOST = "tracer.zookeeper.host";
  public static final String TRACER_ZK_TIMEOUT = "tracer.zookeeper.timeout";
  public static final String TRACER_ZK_PATH = "tracer.zookeeper.path";

  private static final HashSet<SpanReceiver> receivers = new HashSet<>();

  /**
   * @deprecated since 1.7, use {@link DistributedTrace#enable(String, String, org.apache.accumulo.core.client.ClientConfiguration)} instead
   */
  @Deprecated
  public static void enable(Instance instance, ZooReader zoo, String application, String address) throws IOException, KeeperException, InterruptedException {
    enable(address, application);
  }

  /**
   * Enable tracing by setting up SpanReceivers for the current process.
   */
  public static void enable() {
    enable(null, null);
  }

  /**
   * Enable tracing by setting up SpanReceivers for the current process. If service name is null, the simple name of the class will be used.
   */
  public static void enable(String service) {
    enable(null, service);
  }

  /**
   * Enable tracing by setting up SpanReceivers for the current process. If host name is null, it will be determined. If service name is null, the simple name
   * of the class will be used.
   */
  public static void enable(String hostname, String service) {
    enable(hostname, service, ClientConfiguration.loadDefault());
  }

  /**
   * Enable tracing by setting up SpanReceivers for the current process. If host name is null, it will be determined. If service name is null, the simple name
   * of the class will be used. Properties required in the client configuration include
   * {@link org.apache.accumulo.core.client.ClientConfiguration.ClientProperty#TRACE_SPAN_RECEIVERS} and any properties specific to the span receiver.
   */
  public static void enable(String hostname, String service, ClientConfiguration conf) {
    String spanReceivers = conf.get(ClientProperty.TRACE_SPAN_RECEIVERS);
    String zookeepers = conf.get(ClientProperty.INSTANCE_ZK_HOST);
    long timeout = ConfigurationTypeHelper.getTimeInMillis(conf.get(ClientProperty.INSTANCE_ZK_TIMEOUT));
    String zkPath = conf.get(ClientProperty.TRACE_ZK_PATH);
    Map<String,String> properties = conf.getAllPropertiesWithPrefix(ClientProperty.TRACE_SPAN_RECEIVER_PREFIX);
    enableTracing(hostname, service, spanReceivers, zookeepers, timeout, zkPath, properties);
  }

  /**
   * Enable tracing by setting up SpanReceivers for the current process. If host name is null, it will be determined. If service name is null, the simple name
   * of the class will be used.
   */
  public static void enable(String hostname, String service, AccumuloConfiguration conf) {
    String spanReceivers = conf.get(Property.TRACE_SPAN_RECEIVERS);
    String zookeepers = conf.get(Property.INSTANCE_ZK_HOST);
    long timeout = conf.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT);
    String zkPath = conf.get(Property.TRACE_ZK_PATH);
    Map<String,String> properties = conf.getAllPropertiesWithPrefix(Property.TRACE_SPAN_RECEIVER_PREFIX);
    enableTracing(hostname, service, spanReceivers, zookeepers, timeout, zkPath, properties);
  }

  private static void enableTracing(String hostname, String service, String spanReceivers, String zookeepers, long timeout, String zkPath,
      Map<String,String> properties) {
    Configuration conf = new Configuration(false);
    conf.set(Property.TRACE_SPAN_RECEIVERS.toString(), spanReceivers);

    // remaining properties will be parsed through an HTraceConfiguration by SpanReceivers
    setProperty(conf, TRACER_ZK_HOST, zookeepers);
    setProperty(conf, TRACER_ZK_TIMEOUT, (int) timeout);
    setProperty(conf, TRACER_ZK_PATH, zkPath);
    for (Entry<String,String> property : properties.entrySet()) {
      setProperty(conf, property.getKey().substring(Property.TRACE_SPAN_RECEIVER_PREFIX.getKey().length()), property.getValue());
    }
    if (hostname != null) {
      setProperty(conf, TRACE_HOST_PROPERTY, hostname);
    }
    if (service != null) {
      setProperty(conf, TRACE_SERVICE_PROPERTY, service);
    }
    org.apache.htrace.Trace.setProcessId(service);
    ShutdownHookManager.get().addShutdownHook(new Runnable() {
      @Override
      public void run() {
        Trace.off();
        closeReceivers();
      }
    }, 0);
    loadSpanReceivers(conf);
  }

  /**
   * Disable tracing by closing SpanReceivers for the current process.
   */
  public static void disable() {
    closeReceivers();
  }

  private static synchronized void loadSpanReceivers(Configuration conf) {
    if (!receivers.isEmpty()) {
      log.info("Already loaded span receivers, enable tracing does not need to be called again");
      return;
    }
    String[] receiverNames = conf.getTrimmedStrings(Property.TRACE_SPAN_RECEIVERS.toString());
    if (receiverNames == null || receiverNames.length == 0) {
      return;
    }
    for (String className : receiverNames) {
      SpanReceiverBuilder builder = new SpanReceiverBuilder(wrapHadoopConf(conf));
      SpanReceiver rcvr = builder.spanReceiverClass(className.trim()).build();
      if (rcvr == null) {
        log.warn("Failed to load SpanReceiver {}", className);
      } else {
        receivers.add(rcvr);
        log.info("SpanReceiver {} was loaded successfully.", className);
      }
    }
    for (SpanReceiver rcvr : receivers) {
      org.apache.htrace.Trace.addReceiver(rcvr);
    }
  }

  private static void setProperty(Configuration conf, String key, String value) {
    conf.set(HTRACE_CONF_PREFIX + key, value);
  }

  private static void setProperty(Configuration conf, String key, int value) {
    conf.setInt(HTRACE_CONF_PREFIX + key, value);
  }

  private static HTraceConfiguration wrapHadoopConf(final Configuration conf) {
    return new HTraceConfiguration() {
      @Override
      public String get(String key) {
        return conf.get(HTRACE_CONF_PREFIX + key);
      }

      @Override
      public String get(String key, String defaultValue) {
        return conf.get(HTRACE_CONF_PREFIX + key, defaultValue);
      }
    };
  }

  private static synchronized void closeReceivers() {
    for (SpanReceiver rcvr : receivers) {
      try {
        rcvr.close();
      } catch (IOException e) {
        log.warn("Unable to close SpanReceiver correctly: {}", e.getMessage(), e);
      }
    }
    receivers.clear();
  }
}
