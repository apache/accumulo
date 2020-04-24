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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.Properties;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.ServerUtil;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.accumulo.tracer.thrift.RemoteSpan;
import org.apache.accumulo.tracer.thrift.SpanReceiver.Iface;
import org.apache.accumulo.tracer.thrift.SpanReceiver.Processor;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TByteArrayOutputStream;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceServer implements Watcher, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(TraceServer.class);
  private final ServerContext context;
  private final TServer server;
  private final AtomicReference<BatchWriter> writer;
  private final AccumuloClient accumuloClient;
  final String tableName;
  private static final int BATCH_WRITER_MAX_LATENCY = 5;
  private static final long SCHEDULE_PERIOD = 1000;
  private static final long SCHEDULE_DELAY = 1000;

  private static void put(Mutation m, String cf, String cq, byte[] bytes, int len) {
    m.put(new Text(cf), new Text(cq), new Value(bytes, 0, len));
  }

  @Override
  public void close() {
    if (accumuloClient != null) {
      accumuloClient.close();
    }
  }

  static class ByteArrayTransport extends TTransport {
    TByteArrayOutputStream out = new TByteArrayOutputStream();

    @Override
    public boolean isOpen() {
      return true;
    }

    @Override
    public void open() {}

    @Override
    public void close() {}

    @Override
    public int read(byte[] buf, int off, int len) {
      return 0;
    }

    @Override
    public void write(byte[] buf, int off, int len) {
      out.write(buf, off, len);
    }

    public byte[] get() {
      return out.get();
    }

    public int len() {
      return out.len();
    }
  }

  class Receiver implements Iface {
    @Override
    public void span(RemoteSpan s) throws TException {
      String idString = Long.toHexString(s.traceId);
      String startString = Long.toHexString(s.start);
      Mutation spanMutation = new Mutation(new Text(idString));
      Mutation indexMutation = new Mutation(new Text("idx:" + s.svc + ":" + startString));
      long diff = s.stop - s.start;
      indexMutation.put(new Text(s.description), new Text(s.sender),
          new Value(idString + ":" + Long.toHexString(diff)));
      ByteArrayTransport transport = new ByteArrayTransport();
      TCompactProtocol protocol = new TCompactProtocol(transport);
      s.write(protocol);
      String parentString = s.getParentIdsSize() == 0 ? "" : s.getParentIds().stream()
          .map(x -> Long.toHexString(x)).collect(Collectors.toList()).toString();
      put(spanMutation, "span", parentString + ":" + Long.toHexString(s.spanId), transport.get(),
          transport.len());
      // Map the root span to time so we can look up traces by time
      Mutation timeMutation = null;
      if (s.getParentIdsSize() == 0) {
        timeMutation = new Mutation(new Text("start:" + startString));
        put(timeMutation, "id", idString, transport.get(), transport.len());
      }
      try {
        final BatchWriter writer = TraceServer.this.writer.get();
        /*
         * Check for null, because we expect spans to come in much faster than flush calls. In the
         * case of failure, we'd rather avoid logging tons of NPEs.
         */
        if (writer == null) {
          log.warn("writer is not ready; discarding span.");
          return;
        }
        writer.addMutation(spanMutation);
        writer.addMutation(indexMutation);
        if (timeMutation != null) {
          writer.addMutation(timeMutation);
        }
      } catch (MutationsRejectedException exception) {
        log.warn("Unable to write mutation to table; discarding span. set log"
            + " level to DEBUG for span information and stacktrace. cause: " + exception);
        if (log.isDebugEnabled()) {
          log.debug("discarded span due to rejection of mutation: " + spanMutation, exception);
        }
        /*
         * XXX this could be e.g. an IllegalArgumentException if we're trying to write this mutation
         * to a writer that has been closed since we retrieved it
         */
      } catch (RuntimeException exception) {
        log.warn("Unable to write mutation to table; discarding span. set log"
            + " level to DEBUG for stacktrace. cause: " + exception);
        log.debug("unable to write mutation to table due to exception.", exception);
      }
    }

  }

  public TraceServer(ServerContext context, String hostname) throws Exception {
    this.context = context;
    log.info("Version {}", Constants.VERSION);
    log.info("Instance {}", context.getInstanceID());
    AccumuloConfiguration conf = context.getConfiguration();
    tableName = conf.get(Property.TRACE_TABLE);
    accumuloClient = ensureTraceTableExists(conf);

    int[] ports = conf.getPort(Property.TRACE_PORT);
    ServerSocket sock = null;
    for (int port : ports) {
      ServerSocket s = ServerSocketChannel.open().socket();
      s.setReuseAddress(true);
      try {
        s.bind(new InetSocketAddress(hostname, port));
        sock = s;
        break;
      } catch (Exception e) {
        log.warn("Unable to start trace server on port {}", port);
      }
    }
    if (sock == null) {
      throw new RuntimeException(
          "Unable to start trace server on configured ports: " + Arrays.toString(ports));
    }
    final TServerTransport transport = new TServerSocket(sock);
    TThreadPoolServer.Args options = new TThreadPoolServer.Args(transport);
    options.processor(new Processor<Iface>(new Receiver()));
    server = new TThreadPoolServer(options);
    // if sock is bound to the wildcard address, the local address will be 0.0.0.0.
    // check for this, and try using InetAddress.getLocalHost instead.
    // the problem is registering 0.0.0.0 in zookeeper doesn't work for non-local
    // services (like remote tablet servers)
    String hostAddr = sock.getInetAddress().getHostAddress();
    if ("0.0.0.0".equals(hostAddr))
      hostAddr = InetAddress.getLocalHost().getHostAddress();
    registerInZooKeeper(hostAddr + ":" + sock.getLocalPort(), conf.get(Property.TRACE_ZK_PATH));
    writer = new AtomicReference<>(this.accumuloClient.createBatchWriter(tableName,
        new BatchWriterConfig().setMaxLatency(BATCH_WRITER_MAX_LATENCY, TimeUnit.SECONDS)));
  }

  /**
   * Exceptions thrown out of here should be things that cause service failure (e.g.
   * misconfigurations that aren't likely to change on retry).
   *
   * @return a working Connection that can be reused
   * @throws ReflectiveOperationException
   *           if we fail to create an instance of TRACE_TOKEN_TYPE.
   * @throws AccumuloSecurityException
   *           if the trace user has the wrong permissions
   */
  private AccumuloClient ensureTraceTableExists(final AccumuloConfiguration conf)
      throws AccumuloSecurityException, ReflectiveOperationException {
    AccumuloClient accumuloClient = null;
    while (true) {
      try {
        final boolean isDefaultTokenType =
            conf.get(Property.TRACE_TOKEN_TYPE).equals(Property.TRACE_TOKEN_TYPE.getDefaultValue());
        String principal = conf.get(Property.TRACE_USER);
        if (conf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
          // Make sure that we replace _HOST if it exists in the principal
          principal = SecurityUtil.getServerPrincipal(principal);
        }
        AuthenticationToken at;
        Map<String,String> loginMap =
            conf.getAllPropertiesWithPrefix(Property.TRACE_TOKEN_PROPERTY_PREFIX);
        if (loginMap.isEmpty() && isDefaultTokenType) {
          // Assume the old type of user/password specification
          Property p = Property.TRACE_PASSWORD;
          at = new PasswordToken(conf.get(p).getBytes(UTF_8));
        } else {
          Properties props = new Properties();
          AuthenticationToken token =
              AccumuloVFSClassLoader.getClassLoader().loadClass(conf.get(Property.TRACE_TOKEN_TYPE))
                  .asSubclass(AuthenticationToken.class).getDeclaredConstructor().newInstance();

          int prefixLength = Property.TRACE_TOKEN_PROPERTY_PREFIX.getKey().length();
          for (Entry<String,String> entry : loginMap.entrySet()) {
            props.put(entry.getKey().substring(prefixLength), entry.getValue());
          }
          token.init(props);
          at = token;
        }

        accumuloClient =
            Accumulo.newClient().from(context.getProperties()).as(principal, at).build();

        if (!accumuloClient.tableOperations().exists(tableName)) {
          accumuloClient.tableOperations().create(tableName);
          IteratorSetting setting = new IteratorSetting(10, "ageoff", AgeOffFilter.class.getName());
          AgeOffFilter.setTTL(setting, 7 * 24 * 60 * 60 * 1000L);
          accumuloClient.tableOperations().attachIterator(tableName, setting);
        }
        accumuloClient.tableOperations().setProperty(tableName,
            Property.TABLE_FORMATTER_CLASS.getKey(), TraceFormatter.class.getName());
        break;
      } catch (AccumuloException | TableExistsException | TableNotFoundException | IOException
          | RuntimeException ex) {
        log.info("Waiting to checking/create the trace table.", ex);
        sleepUninterruptibly(1, TimeUnit.SECONDS);
        if (accumuloClient != null) {
          accumuloClient.close();
          accumuloClient = null;
        }
      }
    }
    return accumuloClient;
  }

  public void run() {
    SimpleTimer.getInstance(context.getConfiguration()).schedule(() -> flush(), SCHEDULE_DELAY,
        SCHEDULE_PERIOD);
    server.serve();
  }

  private void flush() {
    try {
      final BatchWriter writer = this.writer.get();
      if (writer != null) {
        writer.flush();
      } else {
        // We don't have a writer. If the table exists, try to make a new writer.
        if (accumuloClient.tableOperations().exists(tableName)) {
          resetWriter();
        }
      }
    } catch (MutationsRejectedException | RuntimeException exception) {
      log.warn("Problem flushing traces, resetting writer. Set log level to"
          + " DEBUG to see stacktrace. cause: " + exception);
      log.debug("flushing traces failed due to exception", exception);
      resetWriter();
      /* XXX e.g. if the writer was closed between when we grabbed it and when we called flush. */
    }
  }

  private void resetWriter() {
    BatchWriter writer = null;
    try {
      writer = accumuloClient.createBatchWriter(tableName,
          new BatchWriterConfig().setMaxLatency(BATCH_WRITER_MAX_LATENCY, TimeUnit.SECONDS));
    } catch (Exception ex) {
      log.warn("Unable to create a batch writer, will retry. Set log level to"
          + " DEBUG to see stacktrace. cause: " + ex);
      log.debug("batch writer creation failed with exception.", ex);
    } finally {
      /* Trade in the new writer (even if null) for the one we need to close. */
      writer = this.writer.getAndSet(writer);
      try {
        if (writer != null) {
          writer.close();
        }
      } catch (Exception ex) {
        log.warn(
            "Problem closing batch writer. Set log level to DEBUG to see stacktrace. cause: " + ex);
        log.debug("batch writer close failed with exception", ex);
      }
    }
  }

  private void registerInZooKeeper(String name, String root) throws Exception {
    ZooReaderWriter zoo = context.getZooReaderWriter();
    zoo.putPersistentData(root, new byte[0], NodeExistsPolicy.SKIP);
    log.info("Registering tracer {} at {}", name, root);
    String path = zoo.putEphemeralSequential(root + "/trace-", name.getBytes(UTF_8));
    zoo.exists(path, this);
  }

  private static void loginTracer(AccumuloConfiguration acuConf) {
    try {
      Class<? extends AuthenticationToken> traceTokenType = AccumuloVFSClassLoader.getClassLoader()
          .loadClass(acuConf.get(Property.TRACE_TOKEN_TYPE)).asSubclass(AuthenticationToken.class);

      if (KerberosToken.class.isAssignableFrom(traceTokenType)) {
        // We're using Kerberos to talk to Accumulo, so check for trace user specific auth details.
        // We presume this same user will have the needed access for the service to interact with
        // HDFS/ZK for
        // instance information.
        log.info("Handling login under the assumption that Accumulo users are using Kerberos.");
        Map<String,String> loginMap =
            acuConf.getAllPropertiesWithPrefix(Property.TRACE_TOKEN_PROPERTY_PREFIX);
        String keyTab = loginMap.get(Property.TRACE_TOKEN_PROPERTY_PREFIX.getKey() + "keytab");
        if (keyTab == null || keyTab.isEmpty()) {
          keyTab = acuConf.getPath(Property.GENERAL_KERBEROS_KEYTAB);
        }
        if (keyTab == null || keyTab.isEmpty()) {
          return;
        }

        String principalConfig = acuConf.get(Property.TRACE_USER);
        if (principalConfig == null || principalConfig.isEmpty()) {
          return;
        }

        log.info("Attempting to login as {} with {}", principalConfig, keyTab);
        SecurityUtil.serverLogin(acuConf, keyTab, principalConfig);
      } else {
        // We're not using Kerberos to talk to Accumulo, but we might still need it for talking to
        // HDFS/ZK for
        // instance information.
        log.info("Handling login under the assumption that Accumulo users are not using Kerberos.");
        SecurityUtil.serverLogin(acuConf);
      }
    } catch (IOException | ClassNotFoundException exception) {
      final String msg =
          String.format("Failed to retrieve trace user token information based on property %1s.",
              Property.TRACE_TOKEN_TYPE);
      log.error(msg, exception);
      throw new RuntimeException(msg, exception);
    }
  }

  public static void main(String[] args) throws Exception {
    final String app = "tracer";
    ServerOpts opts = new ServerOpts();
    opts.parseArgs(app, args);
    ServerContext context = new ServerContext(opts.getSiteConfiguration());
    loginTracer(context.getConfiguration());
    ServerUtil.init(context, app);
    try (TraceServer server = new TraceServer(context, opts.getAddress())) {
      server.run();
    } finally {
      log.info("tracer stopping");
      context.getZooReaderWriter().getZooKeeper().close();
    }
  }

  @Override
  public void process(WatchedEvent event) {
    log.debug("event {} {} {}", event.getPath(), event.getType(), event.getState());
    if (event.getState() == KeeperState.Expired) {
      log.warn("Trace server lost zookeeper registration at {} ", event.getPath());
      server.stop();
    } else if (event.getType() == EventType.NodeDeleted) {
      log.warn("Trace server zookeeper entry lost {}", event.getPath());
      server.stop();
    }
    if (event.getPath() != null) {
      try {
        if (context.getZooReaderWriter().exists(event.getPath(), this)) {
          return;
        }
      } catch (Exception ex) {
        log.error("{}", ex.getMessage(), ex);
      }
      log.warn("Trace server unable to reset watch on zookeeper registration");
      server.stop();
    }
  }

}
