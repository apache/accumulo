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
package org.apache.accumulo.proxy;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.Properties;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.proxy.thrift.AccumuloProxy;
import org.apache.accumulo.server.util.RpcWrapper;
import org.apache.log4j.Logger;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.google.common.io.Files;

public class Proxy {

  private static final Logger log = Logger.getLogger(Proxy.class);

  public static class PropertiesConverter implements IStringConverter<Properties> {
    @Override
    public Properties convert(String fileName) {
      Properties prop = new Properties();
      InputStream is;
      try {
        is = new FileInputStream(fileName);
        try {
          prop.load(is);
        } finally {
          is.close();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return prop;
    }
  }

  public static class Opts extends Help {
    @Parameter(names = "-p", required = true, description = "properties file name", converter = PropertiesConverter.class)
    Properties prop;
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(Proxy.class.getName(), args);

    boolean useMini = Boolean.parseBoolean(opts.prop.getProperty("useMiniAccumulo", "false"));
    boolean useMock = Boolean.parseBoolean(opts.prop.getProperty("useMockInstance", "false"));
    String instance = opts.prop.getProperty("instance");
    String zookeepers = opts.prop.getProperty("zookeepers");

    if (!useMini && !useMock && instance == null) {
      System.err.println("Properties file must contain one of : useMiniAccumulo=true, useMockInstance=true, or instance=<instance name>");
      System.exit(1);
    }

    if (instance != null && zookeepers == null) {
      System.err.println("When instance is set in properties file, zookeepers must also be set.");
      System.exit(1);
    }

    if (!opts.prop.containsKey("port")) {
      System.err.println("No port property");
      System.exit(1);
    }

    if (useMini) {
      log.info("Creating mini cluster");
      final File folder = Files.createTempDir();
      final MiniAccumuloCluster accumulo = new MiniAccumuloCluster(folder, "secret");
      accumulo.start();
      opts.prop.setProperty("instance", accumulo.getConfig().getInstanceName());
      opts.prop.setProperty("zookeepers", accumulo.getZooKeepers());
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void start() {
          try {
            accumulo.stop();
          } catch (Exception e) {
            throw new RuntimeException();
          } finally {
            if (!folder.delete())
              log.warn("Unexpected error removing " + folder);
          }
        }
      });
    }

    Class<? extends TProtocolFactory> protoFactoryClass = Class.forName(opts.prop.getProperty("protocolFactory", TCompactProtocol.Factory.class.getName()))
        .asSubclass(TProtocolFactory.class);
    int port = Integer.parseInt(opts.prop.getProperty("port"));
    TServer server = createProxyServer(AccumuloProxy.class, ProxyServer.class, port, protoFactoryClass, opts.prop);
    server.serve();
  }

  public static TServer createProxyServer(Class<?> api, Class<?> implementor, final int port, Class<? extends TProtocolFactory> protoClass,
      Properties properties) throws Exception {
    final TNonblockingServerSocket socket = new TNonblockingServerSocket(port);

    // create the implementor
    Object impl = implementor.getConstructor(Properties.class).newInstance(properties);

    Class<?> proxyProcClass = Class.forName(api.getName() + "$Processor");
    Class<?> proxyIfaceClass = Class.forName(api.getName() + "$Iface");

    @SuppressWarnings("unchecked")
    Constructor<? extends TProcessor> proxyProcConstructor = (Constructor<? extends TProcessor>) proxyProcClass.getConstructor(proxyIfaceClass);

    final TProcessor processor = proxyProcConstructor.newInstance(RpcWrapper.service(impl));

    THsHaServer.Args args = new THsHaServer.Args(socket);
    args.processor(processor);
    final long maxFrameSize = AccumuloConfiguration.getMemoryInBytes(properties.getProperty("maxFrameSize", "16M"));
    if (maxFrameSize > Integer.MAX_VALUE)
      throw new RuntimeException(maxFrameSize + " is larger than MAX_INT");
    args.transportFactory(new TFramedTransport.Factory((int) maxFrameSize));
    args.protocolFactory(protoClass.newInstance());
    args.maxReadBufferBytes = maxFrameSize;
    return new THsHaServer(args);
  }
}
