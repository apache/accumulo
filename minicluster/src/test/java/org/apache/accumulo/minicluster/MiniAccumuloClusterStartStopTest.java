package org.apache.accumulo.minicluster;

import java.io.IOException;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MiniAccumuloClusterStartStopTest {
  
  public TemporaryFolder folder = new TemporaryFolder();
  
  @Before
  public void createMacDir() throws IOException {
    folder.create();
  }
  
  @After
  public void deleteMacDir() {
    folder.delete();
  }
  
  @Test
  public void multipleStartsThrowsAnException() throws Exception {
    MiniAccumuloCluster accumulo = new MiniAccumuloCluster(folder.getRoot(), "superSecret");
    accumulo.start();
    
    try {
      accumulo.start();
      Assert.fail("Invoking start() while already started is an error");
    } catch (IllegalStateException e) {
      // pass
    } finally {
      accumulo.stop();
    }
  }
  
  @Test
  public void multipleStopsIsAllowed() throws Exception {
    MiniAccumuloCluster accumulo = new MiniAccumuloCluster(folder.getRoot(), "superSecret");
    accumulo.start();
    
    Connector conn = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers()).getConnector("root", new PasswordToken("superSecret"));
    conn.tableOperations().create("foo");

    accumulo.stop();
    accumulo.stop();
  }
}
