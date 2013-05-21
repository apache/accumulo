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

import com.google.common.io.Files;
import org.apache.accumulo.mini.MiniAccumuloCluster;
import org.apache.accumulo.proxy.thrift.AccumuloProxy;
import org.apache.log4j.Logger;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Properties;

public class Proxy {
  
  private static final Logger log = Logger.getLogger(Proxy.class);

  public static void main(String[] args) throws Exception {

    if(args.length != 2 || !args[0].equals("-p")) {
        System.err.println("Missing '-p' option with a valid property file");
        System.exit(1);
    }
    
    Properties props = new Properties();
    try {
      FileInputStream is = new FileInputStream(new File(args[1]));
      props.load(is);
      is.close();
    } catch (IOException e) {
      
      System.err.println("There was an error opening the property file");
      System.exit(1);
      
    }

    boolean useMini = Boolean.parseBoolean(props.getProperty("useMiniAccumulo", "false"));
    boolean useMock = Boolean.parseBoolean(props.getProperty("useMockInstance", "false"));
    String instance = props.getProperty("instance");
    String zookeepers = props.getProperty("zookeepers");
    
    if (!useMini && !useMock && instance == null) {
      System.err.println("Properties file must contain one of : useMiniAccumulo=true, useMockInstance=true, or instance=<instance name>");
      System.exit(1);
    }
    
    if (instance != null && zookeepers == null) {
      System.err.println("When instance is set in properties file, zookeepers must also be set.");
      System.exit(1);
    }

    if (!props.containsKey("port")) {
      System.err.println("No port property");
      System.exit(1);
    }

    if (useMini) {
      log.info("Creating mini cluster");
      final File folder = Files.createTempDir();
      final MiniAccumuloCluster accumulo = new MiniAccumuloCluster(folder, "secret");
      accumulo.start();
      props.setProperty("instance", accumulo.getInstanceName());
      props.setProperty("zookeepers", accumulo.getZooKeepers());
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void start() {
          try {
            accumulo.stop();
          } catch (Exception e) {
            throw new RuntimeException();
          } finally {
            folder.delete();
          }
        }
      });
    }

    Class<? extends TProtocolFactory> protoFactoryClass = Class.forName(props.getProperty("protocolFactory", TCompactProtocol.Factory.class.getName()))
        .asSubclass(TProtocolFactory.class);
    int port = Integer.parseInt(props.getProperty("port"));
    TServer server = createProxyServer(AccumuloProxy.class, ProxyServer.class, port, protoFactoryClass, props);
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
    
    final TProcessor processor = proxyProcConstructor.newInstance(impl);
    
    THsHaServer.Args args = new THsHaServer.Args(socket);
    args.processor(processor);
    args.transportFactory(new TFramedTransport.Factory());
    args.protocolFactory(protoClass.newInstance());
    return new THsHaServer(args);
  }
}
