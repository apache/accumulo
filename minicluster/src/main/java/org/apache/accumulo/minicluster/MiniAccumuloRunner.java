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
package org.apache.accumulo.minicluster;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.io.FileUtils;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.google.common.io.Files;

/**
 * A runner for starting up a {@link MiniAccumuloCluster} from the command line using an optional configuration properties file. An example property file looks
 * like the following:
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
 * All items in the properties file above are optional and a default value will be provided in their absence. Any site configuration properties (typically found
 * in the accumulo-site.xml file) should be prefixed with "site." in the properties file.
 * 
 * @since 1.6.0
 */
public class MiniAccumuloRunner {
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
  
  private static final String FORMAT_STRING = "  %-21s %s";
  
  public static class Opts extends Help {
    @Parameter(names = "-p", required = false, description = "properties file name", converter = PropertiesConverter.class)
    Properties prop = new Properties();
  }
  
  /**
   * Runs the {@link MiniAccumuloCluster} given a -p argument with a property file. Establishes a shutdown port for asynchronous operation.
   * 
   * @param args
   *          An optional -p argument can be specified with the path to a valid properties file.
   * 
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(MiniAccumuloRunner.class.getName(), args);
    
    int shutdownPort = 4445;
    
    final File tempDir = Files.createTempDir();
    String rootPass = opts.prop.containsKey("rootPassword") ? opts.prop.getProperty("rootPassword") : "secret";
    
    MiniAccumuloConfig config = new MiniAccumuloConfig(tempDir, rootPass);
    
    if (opts.prop.containsKey("instanceName"))
      config.setInstanceName(opts.prop.getProperty("instanceName"));
    if (opts.prop.containsKey("numTServers"))
      config.setNumTservers(Integer.parseInt(opts.prop.getProperty("numTServers")));
    if (opts.prop.containsKey("zooKeeperPort"))
      config.setZooKeeperPort(Integer.parseInt(opts.prop.getProperty("zooKeeperPort")));
    if (opts.prop.containsKey("jdwpEnabled"))
      config.setJDWPEnabled(Boolean.parseBoolean(opts.prop.getProperty("jdwpEnabled")));
    if (opts.prop.containsKey("zooKeeperMemory"))
      setMemoryOnConfig(config, opts.prop.getProperty("zooKeeperMemory"), ServerType.ZOOKEEPER);
    if (opts.prop.containsKey("tserverMemory"))
      setMemoryOnConfig(config, opts.prop.getProperty("tserverMemory"), ServerType.TABLET_SERVER);
    if (opts.prop.containsKey("masterMemory"))
      setMemoryOnConfig(config, opts.prop.getProperty("masterMemory"), ServerType.MASTER);
    if (opts.prop.containsKey("defaultMemory"))
      setMemoryOnConfig(config, opts.prop.getProperty("defaultMemory"));
    if (opts.prop.containsKey("shutdownPort"))
      shutdownPort = Integer.parseInt(opts.prop.getProperty("shutdownPort"));
    
    Map<String,String> siteConfig = new HashMap<String,String>();
    for (Map.Entry<Object,Object> entry : opts.prop.entrySet()) {
      String key = (String) entry.getKey();
      if (key.startsWith("site."))
        siteConfig.put(key.replaceFirst("site.", ""), (String) entry.getValue());
    }
    
    config.setSiteConfig(siteConfig);
    
    final MiniAccumuloCluster accumulo = new MiniAccumuloCluster(config);
    
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          accumulo.stop();
          FileUtils.deleteDirectory(tempDir);
          System.out.println("\nShut down gracefully on " + new Date());
        } catch (IOException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
    
    accumulo.start();
    
    printInfo(accumulo, shutdownPort);
    
    // start a socket on the shutdown port and block- anything connected to this port will activate the shutdown
    ServerSocket shutdownServer = new ServerSocket(shutdownPort);
    shutdownServer.accept();
    
    System.exit(0);
  }
  
  private static boolean validateMemoryString(String memoryString) {
    String unitsRegex = "[";
    MemoryUnit[] units = MemoryUnit.values();
    for (int i = 0; i < units.length; i++) {
      unitsRegex += units[i].suffix();
      if (i < units.length - 1)
        unitsRegex += "|";
    }
    unitsRegex += "]";
    Pattern p = Pattern.compile("\\d+" + unitsRegex);
    return p.matcher(memoryString).matches();
  }
  
  private static void setMemoryOnConfig(MiniAccumuloConfig config, String memoryString) {
    setMemoryOnConfig(config, memoryString, null);
  }
  
  private static void setMemoryOnConfig(MiniAccumuloConfig config, String memoryString, ServerType serverType) {
    if (!validateMemoryString(memoryString))
      throw new IllegalArgumentException(memoryString + " is not a valid memory string");
    
    long memSize = Long.parseLong(memoryString.substring(0, memoryString.length() - 1));
    MemoryUnit memUnit = MemoryUnit.fromSuffix(memoryString.substring(memoryString.length() - 1));
    
    if (serverType != null)
      config.setMemory(serverType, memSize, memUnit);
    else
      config.setDefaultMemory(memSize, memUnit);
  }
  
  private static void printInfo(MiniAccumuloCluster accumulo, int shutdownPort) {
    System.out.println("Mini Accumulo Cluster\n");
    System.out.println(String.format(FORMAT_STRING, "Directory:", accumulo.getConfig().getDir().getAbsoluteFile()));
    System.out.println(String.format(FORMAT_STRING, "Logs:", accumulo.getConfig().getLogDir().getAbsoluteFile()));
    System.out.println(String.format(FORMAT_STRING, "Instance Name:", accumulo.getConfig().getInstanceName()));
    System.out.println(String.format(FORMAT_STRING, "Root Password:", accumulo.getConfig().getRootPassword()));
    System.out.println(String.format(FORMAT_STRING, "ZooKeeper:", accumulo.getConfig().getZooKeepers()));
    
    for (Pair<ServerType,Integer> pair : accumulo.getDebugPorts()) {
      System.out.println(String.format(FORMAT_STRING, pair.getFirst().prettyPrint() + " JDWP Host:", "localhost:" + pair.getSecond()));
    }
    
    System.out.println(String.format(FORMAT_STRING, "Shutdown Port:", shutdownPort));
    
    System.out.println("\n\nSuccessfully started on " + new Date());
  }
}
