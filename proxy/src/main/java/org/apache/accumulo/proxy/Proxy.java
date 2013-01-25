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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.Properties;

import org.apache.accumulo.core.cli.Help;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportFactory;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

public class Proxy {
  
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
    
    String[] apis = opts.prop.getProperty("accumulo.proxy.apis").split(",");
    if (apis.length == 0) {
      System.err.println("No apis listed in the accumulo.proxy.apis property");
      System.exit(1);
    }
    for (String api : apis) {
      // check existence of properties
      if (!opts.prop.containsKey(api + ".implementor")) {
        System.err.println("No implementor listed in the " + api + ".implementor property");
        System.exit(1);
      }
      if (!opts.prop.containsKey(api + ".port")) {
        System.err.println("No port in the " + api + ".port property");
        System.exit(1);
      }
      
      Class<?> apiclass = Class.forName(api);
      
      Class<?> implementor = Class.forName(opts.prop.getProperty(api + ".implementor"));
      
      Class<? extends TProtocolFactory> protoFactoryClass = Class.forName(opts.prop.getProperty(api + ".protocolFactory")).asSubclass(TProtocolFactory.class);
      Class<? extends TTransportFactory> transportFactoryClass = Class.forName(opts.prop.getProperty(api + ".transportFactory")).asSubclass(
          TTransportFactory.class);

      int port = Integer.parseInt(opts.prop.getProperty(api + ".port"));
      TServer server = createProxyServer(apiclass, implementor, port, protoFactoryClass, transportFactoryClass, opts.prop);
      server.serve();
    }
  }
  
  public static TServer createProxyServer(Class<?> api, Class<?> implementor, final int port, Class<? extends TProtocolFactory> protoClass,
      Class<? extends TTransportFactory> transportFactoryClass, Properties properties) throws Exception {
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
    args.transportFactory(transportFactoryClass.newInstance());
    args.protocolFactory(protoClass.newInstance());
    return new THsHaServer(args);
  }
  
}
