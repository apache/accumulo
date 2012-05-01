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
package org.apache.accumulo.server.trace;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;
import java.util.TimerTask;

import org.apache.accumulo.cloudtrace.instrument.Span;
import org.apache.accumulo.cloudtrace.thrift.RemoteSpan;
import org.apache.accumulo.cloudtrace.thrift.SpanReceiver;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileUtil;
import org.apache.accumulo.core.trace.TraceFormatter;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.server.zookeeper.IZooReaderWriter;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.TByteArrayOutputStream;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;


public class TraceServer implements Watcher {
  
  final private static Logger log = Logger.getLogger(TraceServer.class);
  final private ServerConfiguration serverConfiguration;
  final private TServer server;
  private BatchWriter writer = null;
  private Connector connector;
  final String table;

  private static void put(Mutation m, String cf, String cq, byte[] bytes, int len) {
    m.put(new Text(cf), new Text(cq), new Value(bytes, 0, len));
  }
  
  static class ByteArrayTransport extends TTransport {
    TByteArrayOutputStream out = new TByteArrayOutputStream();
    
    @Override
    public boolean isOpen() {
      return true;
    }
    
    @Override
    public void open() throws TTransportException {}
    
    @Override
    public void close() {}
    
    @Override
    public int read(byte[] buf, int off, int len) {
      return 0;
    }
    
    @Override
    public void write(byte[] buf, int off, int len) throws TTransportException {
      out.write(buf, off, len);
    }
    
    public byte[] get() {
      return out.get();
    }
    
    public int len() {
      return out.len();
    }
  }
  
  class Receiver implements SpanReceiver.Iface {
    @Override
    public void span(RemoteSpan s) throws TException {
      String idString = Long.toHexString(s.traceId);
      String startString = Long.toHexString(s.start);
      Mutation spanMutation = new Mutation(new Text(idString));
      Mutation indexMutation = new Mutation(new Text("idx:" + s.svc + ":" + startString));
      long diff = s.stop - s.start;
      indexMutation.put(new Text(s.description), new Text(s.sender), new Value((idString + ":" + Long.toHexString(diff)).getBytes()));
      ByteArrayTransport transport = new ByteArrayTransport();
      TCompactProtocol protocol = new TCompactProtocol(transport);
      s.write(protocol);
      String parentString = Long.toHexString(s.parentId);
      if (s.parentId == Span.ROOT_SPAN_ID)
        parentString = "";
      put(spanMutation, "span", parentString + ":" + Long.toHexString(s.spanId), transport.get(), transport.len());
      // Map the root span to time so we can look up traces by time
      Mutation timeMutation = null;
      if (s.parentId == Span.ROOT_SPAN_ID) {
        timeMutation = new Mutation(new Text("start:" + startString));
        put(timeMutation, "id", idString, transport.get(), transport.len());
      }
      try {
        if (writer == null)
          resetWriter();
        if (writer == null)
          return;
        writer.addMutation(spanMutation);
        writer.addMutation(indexMutation);
        if (timeMutation != null)
          writer.addMutation(timeMutation);
      } catch (Exception ex) {
        log.error("Unable to write mutation to table: " + spanMutation, ex);
      }
    }
    
  }
  
  public TraceServer(ServerConfiguration serverConfiguration, String hostname) throws Exception {
    this.serverConfiguration = serverConfiguration;
    AccumuloConfiguration conf = serverConfiguration.getConfiguration();
    table = conf.get(Property.TRACE_TABLE);
    while (true) {
      try {
        connector = serverConfiguration.getInstance().getConnector(conf.get(Property.TRACE_USER), conf.get(Property.TRACE_PASSWORD).getBytes());
        if (!connector.tableOperations().exists(table)) {
          connector.tableOperations().create(table);
        }
        connector.tableOperations().setProperty(table, Property.TABLE_FORMATTER_CLASS.getKey(), TraceFormatter.class.getName());
        break;
      } catch (Exception ex) {
        log.info("waiting to checking/create the trace table: " + ex);
        UtilWaitThread.sleep(1000);
      }
    }
    
    int port = conf.getPort(Property.TRACE_PORT);
    final ServerSocket sock = ServerSocketChannel.open().socket();
    sock.setReuseAddress(true);
    sock.bind(new InetSocketAddress(port));
    final TServerTransport transport = new TServerSocket(sock);
    TThreadPoolServer.Args options = new TThreadPoolServer.Args(transport);
    options.processor(new SpanReceiver.Processor(new Receiver()));
    server = new TThreadPoolServer(options);
    final InetSocketAddress address = new InetSocketAddress(hostname, sock.getLocalPort());
    registerInZooKeeper(AddressUtil.toString(address));
    
    writer = connector.createBatchWriter(table, 100l * 1024 * 1024, 5 * 1000l, 10);
  }
  
  public void run() throws Exception {
    SimpleTimer.getInstance().schedule(new TimerTask() {
      @Override
      public void run() {
        flush();
      }
    }, 1000, 1000);
    server.serve();
  }
  
  private void flush() {
    try {
      writer.flush();
    } catch (MutationsRejectedException e) {
      log.error("Error flushing traces", e);
      resetWriter();
    }
  }
  
  synchronized private void resetWriter() {
    try {
      if (writer != null)
        writer.close();
    } catch (Exception ex) {
      log.error("Error closing batch writer", ex);
    } finally {
      writer = null;
      try {
        writer = connector.createBatchWriter(table, 100l * 1024 * 1024, 5 * 1000l, 10);
      } catch (Exception ex) {
        log.error("Unable to create a batch writer: " + ex);
      }
    }
  }
  
  private void registerInZooKeeper(String name) throws Exception {
    String root = ZooUtil.getRoot(serverConfiguration.getInstance()) + Constants.ZTRACERS;
    IZooReaderWriter zoo = ZooReaderWriter.getInstance();
    String path = zoo.putEphemeralSequential(root + "/trace-", name.getBytes());
    zoo.exists(path, this);
  }
  
  public static void main(String[] args) throws Exception {
    SecurityUtil.serverLogin();
    Instance instance = HdfsZooInstance.getInstance();
    ServerConfiguration conf = new ServerConfiguration(instance);
    FileSystem fs = FileUtil.getFileSystem(CachedConfiguration.getInstance(), conf.getConfiguration());
    Accumulo.init(fs, conf, "tracer");
    String hostname = Accumulo.getLocalAddress(args);
    TraceServer server = new TraceServer(conf, hostname);
    Accumulo.enableTracing(hostname, "tserver");
    server.run();
    log.info("tracer stopping");
  }
  
  @Override
  public void process(WatchedEvent event) {
    log.debug("event " + event.getPath() + " " + event.getType() + " " + event.getState());
    if (event.getState() == KeeperState.Expired) {
      log.warn("Logger lost zookeeper registration at " + event.getPath());
      server.stop();
    } else if (event.getType() == EventType.NodeDeleted) {
      log.warn("Logger zookeeper entry lost " + event.getPath());
      server.stop();
    }
  }
  
}
