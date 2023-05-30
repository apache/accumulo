package org.apache.accumulo.test.shell;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.test.shell.ShellIT.StringInputStream;
import org.apache.accumulo.test.shell.ShellIT.TestOutputStream;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Size;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ShellAuthenticatorTest extends SharedMiniClusterBase {

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  private StringInputStream input;
  private TestOutputStream output;
  private Shell shell;
  private File config;
  public LineReader reader;
  public Terminal terminal;
  
  @BeforeEach
  public void setupShell() throws IOException {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    output = new TestOutputStream();
    input = new StringInputStream();
    terminal = new DumbTerminal(input, output);
    terminal.setSize(new Size(80, 24));
    reader = LineReaderBuilder.builder().terminal(terminal).build();
    if (config != null) {
      config.delete();
    }
    config = Files.createTempFile(null, null).toFile();
    try (FileWriter writer = new FileWriter(config)) {
      Properties p = super.getClientProps();
      p.store(writer, null);
    }
    config.deleteOnExit();
  }
  
  @AfterEach
  public void tearDownShell() { 
    if (shell != null) {
      shell.shutdown();
    }
  }

  @Test
  public void testClientPropertiesFile() throws IOException {
    shell = new Shell(reader);
    shell.setLogErrorsToConsole();
    assertTrue(shell.config("--config-file", config.toString()));
  }

  @Test
  public void testClientProperties() throws IOException {
    shell = new Shell(reader);
    shell.setLogErrorsToConsole();
    assertTrue(shell.config("-u", "root", "-p", getRootPassword(), "-zi",
        getCluster().getInstanceName(), "-zh", getCluster().getZooKeepers()));
  }

  @Test
  public void testClientPropertiesBadPassword() throws IOException {
    shell = new Shell(reader);
    shell.setLogErrorsToConsole();
    assertFalse(shell.config("-u", "root", "-p", "BADPW", "-zi",
        getCluster().getInstanceName(), "-zh", getCluster().getZooKeepers()));
  }

  @Test
  public void testAuthTimeoutPropertiesFile() throws IOException, InterruptedException {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    output = new TestOutputStream();
    input = new StringInputStream();
    terminal = new DumbTerminal(input, output);
    terminal.setSize(new Size(80, 24));
    reader = LineReaderBuilder.builder().terminal(terminal).build();
    if (config != null) {
      config.delete();
    }
    config = Files.createTempFile(null, null).toFile();
    try (FileWriter writer = new FileWriter(config)) {
      Properties p = super.getClientProps();
      p.store(writer, null);
    }
    config.deleteOnExit();
    shell = new Shell(reader);
    shell.setLogErrorsToConsole();
   assertTrue(shell.config("--auth-timeout", "2", "--config-file", config.toString()));
    Thread.sleep(90000);
    shell.execCommand("whoami", false, false);
    assertTrue(output.get().contains("root"));
  }

}
