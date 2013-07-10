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
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.server.tabletserver.TabletServer;
import org.apache.accumulo.start.Main;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.junit.Test;

public class HalfDeadTServerIT extends MacTest {
  
  @Override
  public void configure(MiniAccumuloConfig cfg) {
    cfg.setNumTservers(1);
    Map<String,String> siteConfig = new HashMap<String,String>();
    siteConfig.put(Property.INSTANCE_ZK_TIMEOUT.getKey(), "15s");
    siteConfig.put(Property.GENERAL_RPC_TIMEOUT.getKey(), "5s");
    cfg.setSiteConfig(siteConfig );
  }
  
  class DumpOutput extends Daemon {
    
    private final BufferedReader rdr;
    private final StringBuilder output;
    
    DumpOutput(InputStream is) {
      rdr = new BufferedReader(new InputStreamReader(is));
      output = new StringBuilder();
    }
    @Override
    public void run() {
      try {
        while (true) {
          String line = rdr.readLine();
          if (line == null)
            break;
          System.out.println(line);
          output.append(line);
          output.append("\n");
        }
      } catch (IOException ex) {
        log.error(ex, ex);
      }
    }
    
    @Override
    public String toString() {
      return output.toString();
    }
  }
  
  
  @Test(timeout=30*1000)
  public void testRecover() throws Exception {
    test(10);
  }
  
  @Test(timeout=60*1000)
  public void testTimeout() throws Exception {
    String results = test(40);
    if (results != null)
      assertTrue(results.contains("Session expired"));
  }
  
  public String test(int seconds) throws Exception {
    if (!makeDiskFailureLibrary())
      return null;
    Connector c = getConnector();
    assertEquals(1, c.instanceOperations().getTabletServers().size());
    // don't need the regular tablet server
    cluster.killProcess(ServerType.TABLET_SERVER, cluster.getProcesses().get(ServerType.TABLET_SERVER).iterator().next());
    
    // create our own tablet server with the special test library
    String javaHome = System.getProperty("java.home");
    String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
    String classpath = System.getProperty("java.class.path");
    classpath = new File(cluster.getConfig().getDir(), "conf") + File.pathSeparator + classpath;
    String className = TabletServer.class.getCanonicalName();
    ArrayList<String> argList = new ArrayList<String>();
    argList.addAll(Arrays.asList(javaBin, "-cp", classpath));
    argList.addAll(Arrays.asList(Main.class.getName(), className));
    ProcessBuilder builder = new ProcessBuilder(argList);
    Map<String,String> env = builder.environment();
    env.put("ACCUMULO_HOME", cluster.getConfig().getDir().getAbsolutePath());
    env.put("ACCUMULO_LOG_DIR", cluster.getConfig().getLogDir().getAbsolutePath());
    String trickFilename = cluster.getConfig().getLogDir().getAbsolutePath() + "/TRICK_FILE";
    env.put("TRICK_FILE", trickFilename);
    String libPath = System.getProperty("user.dir") + "/target/fake_disk_failure.so";
    env.put("LD_PRELOAD", libPath);
    env.put("DYLD_INSERT_LIBRARIES", libPath);
    env.put("DYLD_FORCE_FLAT_NAMESPACE", "true");
    Process tserver = builder.start();
    DumpOutput t = new DumpOutput(tserver.getInputStream());
    t.start();
    c.tableOperations().create("test_ingest");
    assertTrue(c.instanceOperations().getTabletServers().size() > 1);
    int rows = 100*1000;
    Process ingest = cluster.exec(TestIngest.class, "-u", "root", "-i", cluster.getInstanceName(), "-z", cluster.getZooKeepers(), "-p", MacTest.PASSWORD, "--rows", rows + "");
    UtilWaitThread.sleep(500);
    
    // block I/O with some side-channel trickiness
    File trickFile = new File(trickFilename);
    trickFile.createNewFile();
    UtilWaitThread.sleep(seconds*1000);
    trickFile.delete();

    if (seconds <= 10) {
      assertEquals(0, ingest.waitFor());
      VerifyIngest.Opts vopts = new VerifyIngest.Opts();
      vopts.rows = rows;
      VerifyIngest.verifyIngest(c, vopts, SOPTS);
    } else {
      UtilWaitThread.sleep(5*1000);
    }
    // verify the process was blocked 
    String results = t.toString();
    assertTrue(results.contains("sleeping\nsleeping\nsleeping\n"));
    assertTrue(results.contains("Zookeeper error, will retry"));
    ingest.destroy();
    tserver.destroy();
    t.join();
    return results;
  }

  private boolean makeDiskFailureLibrary() throws Exception {
    String root = System.getProperty("user.dir");
    String source = root + "/src/test/c/fake_disk_failure.c";
    String lib = root + "/target/fake_disk_failure.so";
    String platform = System.getProperty("os.name");
    String cmd[];
    if (platform.equals("Darwin")) {
      cmd = new String[]{"gcc","-arch","x86_64","-arch","i386","-dynamiclib","-O3","-fPIC", source,"-o",lib};
    } else {
      cmd = new String[]{"gcc","-D_GNU_SOURCE","-Wall","-fPIC", source,"-shared", "-o", lib, "-ldl"};
    }
    Process gcc = Runtime.getRuntime().exec(cmd);
    return gcc.waitFor() == 0;
  }
  
}
