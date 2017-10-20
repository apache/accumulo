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
import java.nio.file.Files;
import java.util.Properties;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.rpc.SslConnectionParams;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.proxy.thrift.AccumuloProxy;
import org.apache.accumulo.server.metrics.MetricsFactory;
import org.apache.accumulo.server.rpc.RpcWrapper;
import org.apache.accumulo.server.rpc.SaslServerConnectionParams;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.accumulo.server.rpc.TimedProcessor;
import org.apache.accumulo.server.rpc.UGIAssumingProcessor;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class Proxy implements KeywordExecutable {

  private static final Logger log = LoggerFactory.getLogger(Proxy.class);

  public static final String USE_MINI_ACCUMULO_KEY = "useMiniAccumulo";
  public static final String USE_MINI_ACCUMULO_DEFAULT = "false";
  public static final String USE_MOCK_INSTANCE_KEY = "useMockInstance";
  public static final String USE_MOCK_INSTANCE_DEFAULT = "false";
  public static final String ACCUMULO_INSTANCE_NAME_KEY = "instance";
  public static final String ZOOKEEPERS_KEY = "zookeepers";
  public static final String THRIFT_THREAD_POOL_SIZE_KEY = "numThreads";
  // Default number of threads from THsHaServer.Args
  public static final String THRIFT_THREAD_POOL_SIZE_DEFAULT = "5";
  public static final String THRIFT_MAX_FRAME_SIZE_KEY = "maxFrameSize";
  public static final String THRIFT_MAX_FRAME_SIZE_DEFAULT = "16M";

  // Type of thrift server to create
  public static final String THRIFT_SERVER_TYPE = "thriftServerType";
  public static final String THRIFT_SERVER_TYPE_DEFAULT = "";
  public static final ThriftServerType DEFAULT_SERVER_TYPE = ThriftServerType.getDefault();

  public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";
  public static final String KERBEROS_KEYTAB = "kerberosKeytab";

  public static final String THRIFT_SERVER_HOSTNAME = "thriftServerHostname";
  public static final String THRIFT_SERVER_HOSTNAME_DEFAULT = "0.0.0.0";

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
    @Parameter(names = "-p", description = "properties file name", converter = PropertiesConverter.class)
    Properties prop;
  }

  @Override
  public String keyword() {
    return "proxy";
  }

  @Override
  public UsageGroup usageGroup() {
    return UsageGroup.PROCESS;
  }

  @Override
  public String description() {
    return "Starts Accumulo proxy";
  }

  @Override
  public void execute(final String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(Proxy.class.getName(), args);

    Properties props = new Properties();
    if (opts.prop != null) {
      props = opts.prop;
    } else {
      try (InputStream is = this.getClass().getClassLoader().getResourceAsStream("proxy.properties")) {
        if (is != null) {
          props.load(is);
        } else {
          System.err.println("proxy.properties needs to be specified as argument (using -p) or on the classpath (by putting the file in conf/)");
          System.exit(-1);
        }
      }
    }

    boolean useMini = Boolean.parseBoolean(props.getProperty(USE_MINI_ACCUMULO_KEY, USE_MINI_ACCUMULO_DEFAULT));
    boolean useMock = Boolean.parseBoolean(props.getProperty(USE_MOCK_INSTANCE_KEY, USE_MOCK_INSTANCE_DEFAULT));
    String instance = props.getProperty(ACCUMULO_INSTANCE_NAME_KEY);
    String zookeepers = props.getProperty(ZOOKEEPERS_KEY);

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
      final File folder = Files.createTempDirectory(System.currentTimeMillis() + "").toFile();
      final MiniAccumuloCluster accumulo = new MiniAccumuloCluster(folder, "secret");
      accumulo.start();
      props.setProperty("instance", accumulo.getConfig().getInstanceName());
      props.setProperty("zookeepers", accumulo.getZooKeepers());
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void start() {
          try {
            accumulo.stop();
          } catch (Exception e) {
            throw new RuntimeException();
          } finally {
            if (!folder.delete())
              log.warn("Unexpected error removing {}", folder);
          }
        }
      });
    }

    Class<? extends TProtocolFactory> protoFactoryClass = Class.forName(props.getProperty("protocolFactory", TCompactProtocol.Factory.class.getName()))
        .asSubclass(TProtocolFactory.class);
    TProtocolFactory protoFactory = protoFactoryClass.newInstance();
    int port = Integer.parseInt(props.getProperty("port"));
    String hostname = props.getProperty(THRIFT_SERVER_HOSTNAME, THRIFT_SERVER_HOSTNAME_DEFAULT);
    HostAndPort address = HostAndPort.fromParts(hostname, port);
    ServerAddress server = createProxyServer(address, protoFactory, props);
    // Wait for the server to come up
    while (!server.server.isServing()) {
      Thread.sleep(100);
    }
    log.info("Proxy server started on {}", server.getAddress());
    while (server.server.isServing()) {
      Thread.sleep(1000);
    }
  }

  public static void main(String[] args) throws Exception {
    new Proxy().execute(args);
  }

  public static ServerAddress createProxyServer(HostAndPort address, TProtocolFactory protocolFactory, Properties properties) throws Exception {
    return createProxyServer(address, protocolFactory, properties, ClientConfiguration.loadDefault());
  }

  public static ServerAddress createProxyServer(HostAndPort address, TProtocolFactory protocolFactory, Properties properties, ClientConfiguration clientConf)
      throws Exception {
    final int numThreads = Integer.parseInt(properties.getProperty(THRIFT_THREAD_POOL_SIZE_KEY, THRIFT_THREAD_POOL_SIZE_DEFAULT));
    final long maxFrameSize = ConfigurationTypeHelper.getFixedMemoryAsBytes(properties.getProperty(THRIFT_MAX_FRAME_SIZE_KEY, THRIFT_MAX_FRAME_SIZE_DEFAULT));
    final int simpleTimerThreadpoolSize = Integer.parseInt(Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE.getDefaultValue());
    // How frequently to try to resize the thread pool
    final long threadpoolResizeInterval = 1000l * 5;
    // No timeout
    final long serverSocketTimeout = 0l;
    // Use the new hadoop metrics2 support
    final MetricsFactory metricsFactory = new MetricsFactory(false);
    final String serverName = "Proxy", threadName = "Accumulo Thrift Proxy";

    // create the implementation of the proxy interface
    ProxyServer impl = new ProxyServer(properties);

    // Wrap the implementation -- translate some exceptions
    AccumuloProxy.Iface wrappedImpl = RpcWrapper.service(impl);

    // Create the processor from the implementation
    TProcessor processor = new AccumuloProxy.Processor<>(wrappedImpl);

    // Get the type of thrift server to instantiate
    final String serverTypeStr = properties.getProperty(THRIFT_SERVER_TYPE, THRIFT_SERVER_TYPE_DEFAULT);
    ThriftServerType serverType = DEFAULT_SERVER_TYPE;
    if (!THRIFT_SERVER_TYPE_DEFAULT.equals(serverTypeStr)) {
      serverType = ThriftServerType.get(serverTypeStr);
    }

    SslConnectionParams sslParams = null;
    SaslServerConnectionParams saslParams = null;
    switch (serverType) {
      case SSL:
        sslParams = SslConnectionParams.forClient(ClientContext.convertClientConfig(clientConf));
        break;
      case SASL:
        if (!clientConf.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false)) {
          // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j capability
          log.error("FATAL: SASL thrift server was requested but it is disabled in client configuration");
          throw new RuntimeException("SASL is not enabled in configuration");
        }

        // Kerberos needs to be enabled to use it
        if (!UserGroupInformation.isSecurityEnabled()) {
          // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j capability
          log.error("FATAL: Hadoop security is not enabled");
          throw new RuntimeException();
        }

        // Login via principal and keytab
        final String kerberosPrincipal = properties.getProperty(KERBEROS_PRINCIPAL, ""),
        kerberosKeytab = properties.getProperty(KERBEROS_KEYTAB, "");
        if (StringUtils.isBlank(kerberosPrincipal) || StringUtils.isBlank(kerberosKeytab)) {
          // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j capability
          log.error("FATAL: Kerberos principal and keytab must be provided");
          throw new RuntimeException();
        }
        UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytab);
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        log.info("Logged in as {}", ugi.getUserName());

        // The kerberosPrimary set in the SASL server needs to match the principal we're logged in as.
        final String shortName = ugi.getShortUserName();
        log.info("Setting server primary to {}", shortName);
        clientConf.setProperty(ClientProperty.KERBEROS_SERVER_PRIMARY, shortName);

        KerberosToken token = new KerberosToken();
        saslParams = new SaslServerConnectionParams(clientConf, token, null);

        processor = new UGIAssumingProcessor(processor);

        break;
      default:
        // nothing to do -- no extra configuration necessary
        break;
    }

    // Hook up support for tracing for thrift calls
    TimedProcessor timedProcessor = new TimedProcessor(metricsFactory, processor, serverName, threadName);

    // Create the thrift server with our processor and properties
    ServerAddress serverAddr = TServerUtils.startTServer(serverType, timedProcessor, protocolFactory, serverName, threadName, numThreads,
        simpleTimerThreadpoolSize, threadpoolResizeInterval, maxFrameSize, sslParams, saslParams, serverSocketTimeout, address);

    return serverAddr;
  }
}
