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
import org.apache.accumulo.core.client.impl.ClientConfConverter;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.rpc.SslConnectionParams;
import org.apache.accumulo.core.trace.wrappers.TraceWrap;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.proxy.thrift.AccumuloProxy;
import org.apache.accumulo.server.metrics.MetricsFactory;
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
  public static final String THRIFT_THREAD_POOL_SIZE_KEY = "numThreads";
  // Default number of threads from THsHaServer.Args
  public static final String THRIFT_THREAD_POOL_SIZE_DEFAULT = "5";
  public static final String THRIFT_MAX_FRAME_SIZE_KEY = "maxFrameSize";
  public static final String THRIFT_MAX_FRAME_SIZE_DEFAULT = "16M";

  // Type of thrift server to create
  public static final String THRIFT_SERVER_TYPE = "thriftServerType";
  public static final String THRIFT_SERVER_TYPE_DEFAULT = "";
  public static final ThriftServerType DEFAULT_SERVER_TYPE = ThriftServerType.getDefault();

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
    @Parameter(names = "-p", description = "proxy.properties path",
        converter = PropertiesConverter.class)
    Properties proxyProps;
    @Parameter(names = "-c", description = "accumulo-client.properties path",
        converter = PropertiesConverter.class)
    Properties clientProps;
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

    Properties proxyProps = opts.proxyProps;
    Properties clientProps = opts.clientProps;

    boolean useMini = Boolean
        .parseBoolean(proxyProps.getProperty(USE_MINI_ACCUMULO_KEY, USE_MINI_ACCUMULO_DEFAULT));
    boolean useMock = Boolean
        .parseBoolean(proxyProps.getProperty(USE_MOCK_INSTANCE_KEY, USE_MOCK_INSTANCE_DEFAULT));

    if (!useMini && !useMock && clientProps == null) {
      System.err.println("The '-c' option must be set with an accumulo-client.properties file or"
          + " proxy.properties must contain either useMiniAccumulo=true or useMockInstance=true");
      System.exit(1);
    }

    if (!proxyProps.containsKey("port")) {
      System.err.println("No port property");
      System.exit(1);
    }

    if (useMini) {
      log.info("Creating mini cluster");
      final File folder = Files.createTempDirectory(System.currentTimeMillis() + "").toFile();
      final MiniAccumuloCluster accumulo = new MiniAccumuloCluster(folder, "secret");
      accumulo.start();
      clientProps.setProperty(ClientProperty.INSTANCE_NAME.getKey(),
          accumulo.getConfig().getInstanceName());
      clientProps.setProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(),
          accumulo.getZooKeepers());
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

    Class<? extends TProtocolFactory> protoFactoryClass = Class
        .forName(
            proxyProps.getProperty("protocolFactory", TCompactProtocol.Factory.class.getName()))
        .asSubclass(TProtocolFactory.class);
    TProtocolFactory protoFactory = protoFactoryClass.newInstance();
    int port = Integer.parseInt(proxyProps.getProperty("port"));
    String hostname = proxyProps.getProperty(THRIFT_SERVER_HOSTNAME,
        THRIFT_SERVER_HOSTNAME_DEFAULT);
    HostAndPort address = HostAndPort.fromParts(hostname, port);
    proxyProps.putAll(clientProps);
    ServerAddress server = createProxyServer(address, protoFactory, proxyProps);
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

  public static ServerAddress createProxyServer(HostAndPort address,
      TProtocolFactory protocolFactory, Properties props) throws Exception {
    final int numThreads = Integer
        .parseInt(props.getProperty(THRIFT_THREAD_POOL_SIZE_KEY, THRIFT_THREAD_POOL_SIZE_DEFAULT));
    final long maxFrameSize = ConfigurationTypeHelper.getFixedMemoryAsBytes(
        props.getProperty(THRIFT_MAX_FRAME_SIZE_KEY, THRIFT_MAX_FRAME_SIZE_DEFAULT));
    final int simpleTimerThreadpoolSize = Integer
        .parseInt(Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE.getDefaultValue());
    // How frequently to try to resize the thread pool
    final long threadpoolResizeInterval = 1000L * 5;
    // No timeout
    final long serverSocketTimeout = 0L;
    // Use the new hadoop metrics2 support
    final MetricsFactory metricsFactory = new MetricsFactory(false);
    final String serverName = "Proxy", threadName = "Accumulo Thrift Proxy";

    // create the implementation of the proxy interface
    ProxyServer impl = new ProxyServer(props);

    // Wrap the implementation -- translate some exceptions
    AccumuloProxy.Iface wrappedImpl = TraceWrap.service(impl);

    // Create the processor from the implementation
    TProcessor processor = new AccumuloProxy.Processor<>(wrappedImpl);

    // Get the type of thrift server to instantiate
    final String serverTypeStr = props.getProperty(THRIFT_SERVER_TYPE, THRIFT_SERVER_TYPE_DEFAULT);
    ThriftServerType serverType = DEFAULT_SERVER_TYPE;
    if (!THRIFT_SERVER_TYPE_DEFAULT.equals(serverTypeStr)) {
      serverType = ThriftServerType.get(serverTypeStr);
    }

    SslConnectionParams sslParams = null;
    SaslServerConnectionParams saslParams = null;
    switch (serverType) {
      case SSL:
        sslParams = SslConnectionParams.forClient(ClientConfConverter.toAccumuloConf(props));
        break;
      case SASL:
        if (!ClientProperty.SASL_ENABLED.getBoolean(props)) {
          throw new IllegalStateException("SASL thrift server was requested but 'sasl.enabled' is"
              + " not set to true in configuration");
        }

        // Kerberos needs to be enabled to use it
        if (!UserGroupInformation.isSecurityEnabled()) {
          throw new IllegalStateException("Hadoop security is not enabled");
        }

        // Login via principal and keytab
        final String kerberosPrincipal = ClientProperty.AUTH_USERNAME.getValue(props);
        final String kerberosKeytab = ClientProperty.AUTH_KERBEROS_KEYTAB_PATH.getValue(props);
        if (StringUtils.isBlank(kerberosPrincipal) || StringUtils.isBlank(kerberosKeytab)) {
          throw new IllegalStateException(
              String.format("Kerberos principal '%s' and keytab '%s'" + " must be provided",
                  kerberosPrincipal, kerberosKeytab));
        }
        UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytab);
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        log.info("Logged in as {}", ugi.getUserName());

        // The kerberosPrimary set in the SASL server needs to match the principal we're logged in
        // as.
        final String shortName = ugi.getShortUserName();
        log.info("Setting server primary to {}", shortName);
        props.setProperty(ClientProperty.SASL_KERBEROS_SERVER_PRIMARY.getKey(), shortName);

        KerberosToken token = new KerberosToken();
        saslParams = new SaslServerConnectionParams(props, token, null);
        processor = new UGIAssumingProcessor(processor);

        break;
      default:
        // nothing to do -- no extra configuration necessary
        break;
    }

    // Hook up support for tracing for thrift calls
    TimedProcessor timedProcessor = new TimedProcessor(metricsFactory, processor, serverName,
        threadName);

    // Create the thrift server with our processor and properties

    return TServerUtils.startTServer(serverType, timedProcessor, protocolFactory, serverName,
        threadName, numThreads, simpleTimerThreadpoolSize, threadpoolResizeInterval, maxFrameSize,
        sslParams, saslParams, serverSocketTimeout, address);
  }
}
