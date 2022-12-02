/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.minicluster;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A runner for starting up a {@link MiniAccumuloCluster} from the command line using an optional
 * configuration properties file. An example property file looks like the following:
 *
 * <pre>
 * rootPassword=secret
 * instanceName=testInstance
 * numTServers=1
 * zooKeeperPort=3191
 * jdwpEnabled=true
 * zooKeeperMemory=128M
 * tserverMemory=256M
 * masterMemory=128M
 * defaultMemory=256M
 * shutdownPort=4446
 * site.instance.secret=HUSH
 * </pre>
 *
 * All items in the properties file above are optional and a default value will be provided in their
 * absence. Any site configuration properties (typically found in the accumulo.properties file)
 * should be prefixed with "site." in the properties file.
 *
 * @since 1.6.0
 */
public class MiniAccumuloRunner {
  private static final Logger log = LoggerFactory.getLogger(MiniAccumuloRunner.class);

  private static final String ROOT_PASSWORD_PROP = "rootPassword";
  private static final String SHUTDOWN_PORT_PROP = "shutdownPort";
  private static final String DEFAULT_MEMORY_PROP = "defaultMemory";
  @Deprecated(since = "2.1.0")
  private static final String MASTER_MEMORY_PROP = "masterMemory";
  private static final String MANAGER_MEMORY_PROP = "managerMemory";
  private static final String TSERVER_MEMORY_PROP = "tserverMemory";
  private static final String ZOO_KEEPER_MEMORY_PROP = "zooKeeperMemory";
  private static final String JDWP_ENABLED_PROP = "jdwpEnabled";
  private static final String ZOO_KEEPER_PORT_PROP = "zooKeeperPort";
  private static final String ZOO_KEEPER_STARTUP_TIME_PROP = "zooKeeperStartupTime";
  private static final String NUM_T_SERVERS_PROP = "numTServers";
  private static final String DIRECTORY_PROP = "directory";
  private static final String INSTANCE_NAME_PROP = "instanceName";
  private static final String EXISTING_ZOO_KEEPERS_PROP = "existingZooKeepers";

  private static void printProperties() {
    System.out.println("#mini Accumulo cluster runner properties.");
    System.out.println("#");
    System.out.println("#uncomment following properties to use, properties not"
        + " set will use default or random value");
    System.out.println();
    System.out.println("#" + INSTANCE_NAME_PROP + "=devTest");
    System.out.println("#" + DIRECTORY_PROP + "=/tmp/mac1");
    System.out.println("#" + ROOT_PASSWORD_PROP + "=secret");
    System.out.println("#" + NUM_T_SERVERS_PROP + "=2");
    System.out.println("#" + ZOO_KEEPER_PORT_PROP + "=40404");
    System.out.println("#" + ZOO_KEEPER_STARTUP_TIME_PROP + "=39000");
    System.out.println("#" + SHUTDOWN_PORT_PROP + "=41414");
    System.out.println("#" + DEFAULT_MEMORY_PROP + "=128M");
    System.out.println("#" + MANAGER_MEMORY_PROP + "=128M");
    System.out.println("#" + TSERVER_MEMORY_PROP + "=128M");
    System.out.println("#" + ZOO_KEEPER_MEMORY_PROP + "=128M");
    System.out.println("#" + JDWP_ENABLED_PROP + "=false");
    System.out.println("#" + EXISTING_ZOO_KEEPERS_PROP + "=localhost:2181");

    System.out.println();
    System.out.println("# Configuration normally placed in accumulo.properties can be added using"
        + " a site.* prefix.");
    System.out.println("# For example the following line will set tserver.compaction.major.delay");
    System.out.println();
    System.out.println("#site.tserver.compaction.major.delay=60s");

  }

  public static class PropertiesConverter implements IStringConverter<Properties> {
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
        justification = "code runs in same security context as user who provided input file name")
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

  private static final String FORMAT_STRING = "  %-21s %s%n";

  public static class Opts extends Help {
    @Parameter(names = "-p", required = false, description = "properties file name",
        converter = PropertiesConverter.class)
    Properties prop = new Properties();

    @Parameter(names = {"-c", "--printProperties"}, required = false,
        description = "prints an example properties file, redirect to file to use")
    boolean printProps = false;
  }

  /**
   * Runs the {@link MiniAccumuloCluster} given a -p argument with a property file. Establishes a
   * shutdown port for asynchronous operation.
   *
   * @param args An optional -p argument can be specified with the path to a valid properties file.
   */
  @SuppressFBWarnings(value = {"PATH_TRAVERSAL_IN", "UNENCRYPTED_SERVER_SOCKET"},
      justification = "code runs in same security context as user who provided input file name; "
          + "socket need not be encrypted, since this class is provided for testing only")
  public static void main(String[] args) throws IOException, InterruptedException {
    Opts opts = new Opts();
    opts.parseArgs(MiniAccumuloRunner.class.getName(), args);

    if (opts.printProps) {
      printProperties();
      System.exit(0);
    }

    int shutdownPort = 4445;

    final File miniDir;

    if (opts.prop.containsKey(DIRECTORY_PROP)) {
      miniDir = new File(opts.prop.getProperty(DIRECTORY_PROP));
    } else {
      miniDir = Files.createTempDirectory(System.currentTimeMillis() + "").toFile();
    }

    String rootPass = opts.prop.containsKey(ROOT_PASSWORD_PROP)
        ? opts.prop.getProperty(ROOT_PASSWORD_PROP) : "secret";

    MiniAccumuloConfig config = new MiniAccumuloConfig(miniDir, rootPass);

    if (opts.prop.containsKey(INSTANCE_NAME_PROP)) {
      config.setInstanceName(opts.prop.getProperty(INSTANCE_NAME_PROP));
    }
    if (opts.prop.containsKey(NUM_T_SERVERS_PROP)) {
      config.setNumTservers(Integer.parseInt(opts.prop.getProperty(NUM_T_SERVERS_PROP)));
    }
    if (opts.prop.containsKey(ZOO_KEEPER_PORT_PROP)) {
      config.setZooKeeperPort(Integer.parseInt(opts.prop.getProperty(ZOO_KEEPER_PORT_PROP)));
    }
    if (opts.prop.containsKey(ZOO_KEEPER_STARTUP_TIME_PROP)) {
      config.setZooKeeperStartupTime(
          Long.parseLong(opts.prop.getProperty(ZOO_KEEPER_STARTUP_TIME_PROP)));
    }
    if (opts.prop.containsKey(EXISTING_ZOO_KEEPERS_PROP)) {
      config.getImpl().setExistingZooKeepers(opts.prop.getProperty(EXISTING_ZOO_KEEPERS_PROP));
    }
    if (opts.prop.containsKey(JDWP_ENABLED_PROP)) {
      config.setJDWPEnabled(Boolean.parseBoolean(opts.prop.getProperty(JDWP_ENABLED_PROP)));
    }
    if (opts.prop.containsKey(ZOO_KEEPER_MEMORY_PROP)) {
      setMemoryOnConfig(config, opts.prop.getProperty(ZOO_KEEPER_MEMORY_PROP),
          ServerType.ZOOKEEPER);
    }
    if (opts.prop.containsKey(TSERVER_MEMORY_PROP)) {
      setMemoryOnConfig(config, opts.prop.getProperty(TSERVER_MEMORY_PROP),
          ServerType.TABLET_SERVER);
    }
    if (opts.prop.containsKey(MASTER_MEMORY_PROP)) {
      log.warn("{} is deprecated. Use {} instead.", MASTER_MEMORY_PROP, MANAGER_MEMORY_PROP);
      setMemoryOnConfig(config, opts.prop.getProperty(MASTER_MEMORY_PROP), ServerType.MANAGER);
    }
    if (opts.prop.containsKey(MANAGER_MEMORY_PROP)) {
      setMemoryOnConfig(config, opts.prop.getProperty(MANAGER_MEMORY_PROP), ServerType.MANAGER);
    }
    if (opts.prop.containsKey(DEFAULT_MEMORY_PROP)) {
      setMemoryOnConfig(config, opts.prop.getProperty(DEFAULT_MEMORY_PROP));
    }
    if (opts.prop.containsKey(SHUTDOWN_PORT_PROP)) {
      shutdownPort = Integer.parseInt(opts.prop.getProperty(SHUTDOWN_PORT_PROP));
    }

    Map<String,String> siteConfig = new HashMap<>();
    for (Map.Entry<Object,Object> entry : opts.prop.entrySet()) {
      String key = (String) entry.getKey();
      if (key.startsWith("site.")) {
        siteConfig.put(key.replaceFirst("site.", ""), (String) entry.getValue());
      }
    }

    config.setSiteConfig(siteConfig);

    final MiniAccumuloCluster accumulo = new MiniAccumuloCluster(config);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        accumulo.stop();
      } catch (IOException e) {
        log.error("IOException attempting to stop Accumulo.", e);
        return;
      } catch (InterruptedException e) {
        log.error("InterruptedException attempting to stop Accumulo.", e);
        return;
      }

      try {
        FileUtils.deleteDirectory(miniDir);
      } catch (IOException e) {
        log.error("IOException attempting to clean up miniDir.", e);
        return;
      }

      System.out.println("\nShut down gracefully on " + new Date());
    }));

    accumulo.start();

    printInfo(accumulo, shutdownPort);

    // start a socket on the shutdown port and block- anything connected to this port will activate
    // the shutdown
    try (ServerSocket shutdownServer = new ServerSocket(shutdownPort)) {
      shutdownServer.accept().close();
    }

    System.exit(0);
  }

  private static boolean validateMemoryString(String memoryString) {
    String unitsRegex = "[";
    MemoryUnit[] units = MemoryUnit.values();
    for (int i = 0; i < units.length; i++) {
      unitsRegex += units[i].suffix();
      if (i < units.length - 1) {
        unitsRegex += "|";
      }
    }
    unitsRegex += "]";
    Pattern p = Pattern.compile("\\d+" + unitsRegex);
    return p.matcher(memoryString).matches();
  }

  private static void setMemoryOnConfig(MiniAccumuloConfig config, String memoryString) {
    setMemoryOnConfig(config, memoryString, null);
  }

  private static void setMemoryOnConfig(MiniAccumuloConfig config, String memoryString,
      ServerType serverType) {
    if (!validateMemoryString(memoryString)) {
      throw new IllegalArgumentException(memoryString + " is not a valid memory string");
    }

    long memSize = Long.parseLong(memoryString.substring(0, memoryString.length() - 1));
    MemoryUnit memUnit = MemoryUnit.fromSuffix(memoryString.substring(memoryString.length() - 1));

    if (serverType != null) {
      config.setMemory(serverType, memSize, memUnit);
    } else {
      config.setDefaultMemory(memSize, memUnit);
    }
  }

  private static void printInfo(MiniAccumuloCluster accumulo, int shutdownPort) {
    System.out.println("Mini Accumulo Cluster\n");
    System.out.printf(FORMAT_STRING, "Directory:", accumulo.getConfig().getDir().getAbsoluteFile());
    System.out.printf(FORMAT_STRING, "Logs:",
        accumulo.getConfig().getImpl().getLogDir().getAbsoluteFile());
    System.out.printf(FORMAT_STRING, "Instance Name:", accumulo.getConfig().getInstanceName());
    System.out.printf(FORMAT_STRING, "Root Password:", accumulo.getConfig().getRootPassword());
    System.out.printf(FORMAT_STRING, "ZooKeeper:", accumulo.getZooKeepers());

    for (Pair<ServerType,Integer> pair : accumulo.getDebugPorts()) {
      System.out.printf(FORMAT_STRING, pair.getFirst().prettyPrint() + " JDWP Host:",
          "localhost:" + pair.getSecond());
    }

    System.out.printf(FORMAT_STRING, "Shutdown Port:", shutdownPort);

    System.out.println();
    System.out.println("  To connect with shell, use the following command : ");
    System.out.println("    accumulo shell -zh " + accumulo.getZooKeepers() + " -zi "
        + accumulo.getConfig().getInstanceName() + " -u root ");

    System.out.println("\n\nSuccessfully started on " + new Date());
  }
}
