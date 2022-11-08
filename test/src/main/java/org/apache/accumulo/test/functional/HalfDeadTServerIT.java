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
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.start.Main;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This test verifies tserver behavior when the node experiences certain failure states that cause
 * it to be unresponsive to network I/O and/or disk access.
 *
 * <p>
 * This failure state is simulated in a tserver by running that tserver with a shared library that
 * hooks the read/write system calls. When the shared library sees a specific trigger file, it
 * pauses the system calls and prints "sleeping", until that file is deleted, after which it proxies
 * the system call to the real system implementation.
 *
 * <p>
 * In response to failures of the type this test simulates, the tserver should recover if the system
 * call is stalled for less than the ZooKeeper timeout. Otherwise, it should lose its lock in
 * ZooKeeper and terminate itself.
 */
public class HalfDeadTServerIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // configure only one tserver from mini; mini won't less us configure 0, so instead, we will
    // start only 1, and kill it to start our own in the desired simulation environment
    cfg.setNumTservers(1);
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.GENERAL_RPC_TIMEOUT, "5s");
    cfg.setProperty(Property.TSERV_NATIVEMAP_ENABLED, Boolean.FALSE.toString());
  }

  private static final AtomicBoolean sharedLibBuilt = new AtomicBoolean(false);

  @SuppressFBWarnings(value = "COMMAND_INJECTION",
      justification = "command executed is not from user input")
  @BeforeAll
  public static void buildSharedLib() throws IOException, InterruptedException {
    String root = System.getProperty("user.dir");
    String source = root + "/src/test/c/fake_disk_failure.c";
    String lib = root + "/target/fake_disk_failure.so";
    String platform = System.getProperty("os.name");
    String[] cmd;
    if (platform.equals("Darwin")) {
      cmd = new String[] {"gcc", "-arch", "x86_64", "-arch", "i386", "-dynamiclib", "-O3", "-fPIC",
          source, "-o", lib};
    } else {
      cmd = new String[] {"gcc", "-D_GNU_SOURCE", "-Wall", "-fPIC", source, "-shared", "-o", lib,
          "-ldl"};
    }
    // inherit IO to link see the command's output on the current console
    Process gcc = new ProcessBuilder(cmd).inheritIO().start();
    // wait for and record whether the compilation of the native library succeeded
    sharedLibBuilt.set(gcc.waitFor() == 0);
  }

  // a simple class to capture a launched process' output (and repeat it back)
  private static class DumpOutput extends Thread {

    private final Scanner lineScanner;
    private final StringBuilder capturedOutput;
    private final PrintStream printer;
    private final String printerName;

    DumpOutput(InputStream is, PrintStream out, String name) {
      lineScanner = new Scanner(is);
      capturedOutput = new StringBuilder();
      printer = out;
      printerName = name;
      this.setDaemon(true);
    }

    @Override
    public void run() {
      while (lineScanner.hasNextLine()) {
        String line = lineScanner.nextLine();
        capturedOutput.append(line);
        capturedOutput.append("\n");
        printer.printf("%s(%s):%s%n", getClass().getSimpleName(), printerName, line);
      }
    }

    public String getCaptured() {
      return capturedOutput.toString();
    }
  }

  @Test
  public void testRecover() throws Exception {
    test(10, false);
  }

  @Test
  public void testTimeout() throws Exception {
    String results = test(20, true);
    if (results != null) {
      if (!results.contains("Session expired")) {
        log.info(
            "Failed to find 'Session expired' in output, but TServer did die which is expected");
      }
    }
  }

  @SuppressFBWarnings(value = {"PATH_TRAVERSAL_IN", "COMMAND_INJECTION"},
      justification = "path provided by test; command args provided by test")
  public String test(int seconds, boolean expectTserverDied) throws Exception {
    assumeTrue(sharedLibBuilt.get(), "Shared library did not build");
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      while (client.instanceOperations().getTabletServers().isEmpty()) {
        // wait until the tserver that we need to kill is running
        Thread.sleep(50);
      }

      // create our own tablet server with the special test library
      String javaHome = System.getProperty("java.home");
      String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
      String classpath = System.getProperty("java.class.path");
      classpath = new File(cluster.getConfig().getDir(), "conf") + File.pathSeparator + classpath;
      String className = TabletServer.class.getName();
      ProcessBuilder builder = new ProcessBuilder(javaBin, Main.class.getName(), className);
      Map<String,String> env = builder.environment();
      env.put("CLASSPATH", classpath);
      env.put("ACCUMULO_HOME", cluster.getConfig().getDir().getAbsolutePath());
      env.put("ACCUMULO_LOG_DIR", cluster.getConfig().getLogDir().getAbsolutePath());
      String trickFilename = cluster.getConfig().getLogDir().getAbsolutePath() + "/TRICK_FILE";
      env.put("TRICK_FILE", trickFilename);
      String libPath = System.getProperty("user.dir") + "/target/fake_disk_failure.so";
      env.put("LD_PRELOAD", libPath);
      env.put("DYLD_INSERT_LIBRARIES", libPath);
      env.put("DYLD_FORCE_FLAT_NAMESPACE", "true");
      Process ingest = null;
      Process tserver = builder.start();
      DumpOutput stderrCollector = new DumpOutput(tserver.getErrorStream(), System.err, "stderr");
      DumpOutput stdoutCollector = new DumpOutput(tserver.getInputStream(), System.out, "stdout");
      try {
        stderrCollector.start();
        stdoutCollector.start();
        sleepUninterruptibly(1, TimeUnit.SECONDS);
        // don't need the regular tablet server
        cluster.killProcess(ServerType.TABLET_SERVER,
            cluster.getProcesses().get(ServerType.TABLET_SERVER).iterator().next());
        sleepUninterruptibly(1, TimeUnit.SECONDS);
        client.tableOperations().create("test_ingest");
        assertEquals(1, client.instanceOperations().getTabletServers().size());
        int rows = 100_000;
        ingest =
            cluster.exec(TestIngest.class, "-c", cluster.getClientPropsPath(), "--rows", rows + "")
                .getProcess();
        sleepUninterruptibly(500, TimeUnit.MILLISECONDS);

        // block I/O with some side-channel trickiness
        File trickFile = new File(trickFilename);
        try {
          assertTrue(trickFile.createNewFile());
          sleepUninterruptibly(seconds, TimeUnit.SECONDS);
        } finally {
          if (!trickFile.delete()) {
            log.error("Couldn't delete {}", trickFile);
          }
        }

        if (seconds <= 10) {
          assertEquals(0, ingest.waitFor());
          VerifyIngest.VerifyParams params = new VerifyIngest.VerifyParams(getClientProperties());
          params.rows = rows;
          VerifyIngest.verifyIngest(client, params);
        } else {
          sleepUninterruptibly(5, TimeUnit.SECONDS);
          tserver.waitFor();
          stderrCollector.join();
          stdoutCollector.join();
          tserver = null;
        }
        // verify the process was blocked
        String results = stdoutCollector.getCaptured();
        assertTrue(results.contains("sleeping\nsleeping\nsleeping\n"));
        return results;
      } finally {
        if (ingest != null) {
          ingest.destroy();
          ingest.waitFor();
        }
        if (tserver != null) {
          try {
            if (expectTserverDied) {
              try {
                tserver.exitValue();
              } catch (IllegalThreadStateException e) {
                fail("Expected TServer to kill itself, but it is still running");
              }
            }
          } finally {
            tserver.destroy();
            tserver.waitFor();
            stderrCollector.join();
            stdoutCollector.join();
          }
        }
      }
    }
  }
}
