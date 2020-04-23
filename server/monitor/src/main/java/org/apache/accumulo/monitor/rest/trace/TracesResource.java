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
package org.apache.accumulo.monitor.rest.trace;

import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.monitor.util.ParameterValidator.ALPHA_NUM_REGEX;
import static org.apache.accumulo.monitor.util.ParameterValidator.RESOURCE_REGEX;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import javax.inject.Inject;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.Properties;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.tracer.SpanTree;
import org.apache.accumulo.tracer.TraceDump;
import org.apache.accumulo.tracer.TraceFormatter;
import org.apache.accumulo.tracer.thrift.Annotation;
import org.apache.accumulo.tracer.thrift.RemoteSpan;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Generates a list of traces with the summary, by type, and trace details
 *
 * @since 2.0.0
 */
@Path("/trace")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class TracesResource {

  @Inject
  private Monitor monitor;

  /**
   * Generates a trace summary
   *
   * @param minutes
   *          Range of minutes to filter traces Min of 0 minutes, Max of 30 days
   * @return Trace summary in specified range
   */
  @Path("summary/{minutes}")
  @GET
  public RecentTracesList getTraces(
      @DefaultValue("10") @PathParam("minutes") @NotNull @Min(0) @Max(2592000) int minutes)
      throws Exception {

    RecentTracesList recentTraces = new RecentTracesList();

    Pair<AccumuloClient,UserGroupInformation> pair = getClient();
    AccumuloClient client = pair.getFirst();
    if (client == null) {
      return recentTraces;
    }
    try {
      final Scanner scanner = getScanner(client);
      if (scanner == null) {
        return recentTraces;
      }

      Range range = getRangeForTrace(minutes);
      scanner.setRange(range);

      final Map<String,RecentTracesInformation> summary = new TreeMap<>();
      if (pair.getSecond() != null) {
        pair.getSecond().doAs((PrivilegedAction<Void>) () -> {
          parseSpans(scanner, summary);
          return null;
        });
      } else {
        parseSpans(scanner, summary);
      }

      // Adds the traces to the list
      for (Entry<String,RecentTracesInformation> entry : summary.entrySet()) {
        RecentTracesInformation stat = entry.getValue();
        recentTraces.addTrace(stat);
      }
      return recentTraces;
    } finally {
      client.close();
    }
  }

  /**
   * Generates a list of traces filtered by type and range of minutes
   *
   * @param type
   *          Type of the trace
   * @param minutes
   *          Range of minutes, Min of 0 and Max 0f 30 days
   * @return List of traces filtered by type and range
   */
  @Path("listType/{type}/{minutes}")
  @GET
  public TraceType getTracesType(
      @PathParam("type") @NotNull @Pattern(regexp = RESOURCE_REGEX) final String type,
      @PathParam("minutes") @Min(0) @Max(2592000) int minutes) throws Exception {

    TraceType typeTraces = new TraceType(type);

    Pair<AccumuloClient,UserGroupInformation> pair = getClient();
    AccumuloClient client = pair.getFirst();
    if (client == null) {
      return typeTraces;
    }
    try {
      final Scanner scanner = getScanner(client);
      if (scanner == null) {
        return typeTraces;
      }

      Range range = getRangeForTrace(minutes);

      scanner.setRange(range);

      if (pair.getSecond() != null) {
        pair.getSecond().doAs((PrivilegedAction<Void>) () -> {
          for (Entry<Key,Value> entry : scanner) {
            RemoteSpan span = TraceFormatter.getRemoteSpan(entry);

            if (span.description.equals(type)) {
              typeTraces.addTrace(new TracesForTypeInformation(span));
            }
          }
          return null;
        });
      } else {
        for (Entry<Key,Value> entry : scanner) {
          RemoteSpan span = TraceFormatter.getRemoteSpan(entry);
          if (span.description.equals(type)) {
            typeTraces.addTrace(new TracesForTypeInformation(span));
          }
        }
      }
      return typeTraces;
    } finally {
      client.close();
    }
  }

  /**
   * Generates a list of traces filtered by ID
   *
   * @param id
   *          ID of the trace to display
   * @return traces by ID
   */
  @Path("show/{id}")
  @GET
  public TraceList getTracesType(
      @PathParam("id") @NotNull @Pattern(regexp = ALPHA_NUM_REGEX) String id) throws Exception {

    TraceList traces = new TraceList(id);

    Pair<AccumuloClient,UserGroupInformation> pair = getClient();
    AccumuloClient client = pair.getFirst();
    if (client == null) {
      return traces;
    }
    try {
      final Scanner scanner = getScanner(client);

      if (scanner == null) {
        return traces;
      }

      Range range = new Range(new Text(id));
      scanner.setRange(range);
      final SpanTree tree = new SpanTree();
      long start;

      if (pair.getSecond() != null) {
        start = pair.getSecond()
            .doAs((PrivilegedAction<Long>) () -> addSpans(scanner, tree, Long.MAX_VALUE));
      } else {
        start = addSpans(scanner, tree, Long.MAX_VALUE);
      }

      traces.addStartTime(start);

      final long finalStart = start;
      Set<Long> visited = tree
          .visit((level, node) -> traces.addTrace(addTraceInformation(level, node, finalStart)));
      tree.nodes.keySet().removeAll(visited);
      if (!tree.nodes.isEmpty()) {
        for (RemoteSpan span : TraceDump.sortByStart(tree.nodes.values())) {
          traces.addTrace(addTraceInformation(0, span, finalStart));
        }
      }
      return traces;
    } finally {
      client.close();
    }
  }

  private static TraceInformation addTraceInformation(int level, RemoteSpan node, long finalStart) {

    boolean hasData = node.data != null && !node.data.isEmpty();
    boolean hasAnnotations = node.annotations != null && !node.annotations.isEmpty();

    AddlInformation addlData = new AddlInformation();

    if (hasData || hasAnnotations) {

      if (hasData) {
        for (Entry<String,String> entry : node.data.entrySet()) {
          DataInformation data = new DataInformation(entry.getKey(), entry.getValue());
          addlData.addData(data);
        }
      }
      if (hasAnnotations) {
        for (Annotation entry : node.annotations) {
          AnnotationInformation annotations =
              new AnnotationInformation(entry.getMsg(), entry.getTime() - finalStart);
          addlData.addAnnotations(annotations);
        }
      }
    }

    return new TraceInformation(level, node.stop - node.start, node.start - finalStart, node.spanId,
        node.svc + "@" + node.sender, node.description, addlData);
  }

  protected Range getRangeForTrace(long minutesSince) {
    long endTime = System.currentTimeMillis();
    long millisSince = minutesSince * 60 * 1000;
    // Catch the overflow
    if (millisSince < minutesSince) {
      millisSince = endTime;
    }
    long startTime = endTime - millisSince;

    String startHexTime = Long.toHexString(startTime), endHexTime = Long.toHexString(endTime);
    return new Range(new Text("start:" + startHexTime), new Text("start:" + endHexTime));
  }

  private void parseSpans(Scanner scanner, Map<String,RecentTracesInformation> summary) {
    for (Entry<Key,Value> entry : scanner) {
      RemoteSpan span = TraceFormatter.getRemoteSpan(entry);
      RecentTracesInformation stats = summary.get(span.description);
      if (stats == null) {
        summary.put(span.description, stats = new RecentTracesInformation(span.description));
      }
      stats.addSpan(span);
    }
  }

  protected Pair<AccumuloClient,UserGroupInformation> getClient() {
    AccumuloConfiguration conf = monitor.getContext().getConfiguration();
    final boolean saslEnabled = conf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED);
    UserGroupInformation traceUgi = null;
    final String principal;
    final AuthenticationToken at;
    Map<String,String> loginMap =
        conf.getAllPropertiesWithPrefix(Property.TRACE_TOKEN_PROPERTY_PREFIX);
    // May be null
    String keytab = loginMap.get(Property.TRACE_TOKEN_PROPERTY_PREFIX.getKey() + "keytab");
    if (keytab == null || keytab.isEmpty()) {
      keytab = conf.getPath(Property.GENERAL_KERBEROS_KEYTAB);
    }

    if (saslEnabled && keytab != null) {
      principal = SecurityUtil.getServerPrincipal(conf.get(Property.TRACE_USER));
      try {
        traceUgi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
      } catch (IOException e) {
        throw new RuntimeException("Failed to login as trace user", e);
      }
    } else {
      principal = conf.get(Property.TRACE_USER);
    }

    if (saslEnabled) {
      at = null;
    } else {
      if (loginMap.isEmpty()) {
        Property p = Property.TRACE_PASSWORD;
        at = new PasswordToken(conf.get(p).getBytes(UTF_8));
      } else {
        Properties props = new Properties();
        int prefixLength = Property.TRACE_TOKEN_PROPERTY_PREFIX.getKey().length();
        for (Entry<String,String> entry : loginMap.entrySet()) {
          props.put(entry.getKey().substring(prefixLength), entry.getValue());
        }

        AuthenticationToken token = Property.createInstanceFromPropertyName(conf,
            Property.TRACE_TOKEN_TYPE, AuthenticationToken.class, new PasswordToken());
        token.init(props);
        at = token;
      }
    }

    java.util.Properties props = monitor.getContext().getProperties();
    AccumuloClient client;
    if (traceUgi != null) {
      try {
        client = traceUgi.doAs((PrivilegedExceptionAction<AccumuloClient>) () -> {
          // Make the KerberosToken inside the doAs
          AuthenticationToken token = new KerberosToken();
          return Accumulo.newClient().from(props).as(principal, token).build();
        });
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException("Failed to obtain scanner", e);
      }
    } else {
      if (at == null) {
        throw new AssertionError("AuthenticationToken should not be null");
      }
      client = Accumulo.newClient().from(props).as(principal, at).build();
    }
    return new Pair<>(client, traceUgi);
  }

  private Scanner getScanner(AccumuloClient client) throws AccumuloException {
    try {
      AccumuloConfiguration conf = monitor.getContext().getConfiguration();
      final String table = conf.get(Property.TRACE_TABLE);
      if (!client.tableOperations().exists(table)) {
        return null;
      }
      return client.createScanner(table);
    } catch (AccumuloSecurityException | TableNotFoundException ex) {
      return null;
    }
  }

  private long addSpans(Scanner scanner, SpanTree tree, long start) {
    for (Entry<Key,Value> entry : scanner) {
      RemoteSpan span = TraceFormatter.getRemoteSpan(entry);
      tree.addNode(span);
      start = min(start, span.start);
    }
    return start;
  }
}
