package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Test;

public class SessionDurabilityIT extends ConfigurableMacIT {
  
  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "5s");
  }
  
  @Test(timeout = 3 * 60 * 1000)
  public void nondurableTableHasDurableWrites() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    // table default has no durability
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_DURABILITY.getKey(), "none");
    // send durable writes
    BatchWriterConfig cfg = new BatchWriterConfig();
    cfg.setDurability(Durability.SYNC);
    writeSome(tableName, 10, cfg);
    assertEquals(10, count(tableName));
    // verify writes servive restart
    restartTServer();
    assertEquals(10, count(tableName));
  }
  
  @Test(timeout = 3 * 60 * 1000)
  public void durableTableLosesNonDurableWrites() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    // table default is durable writes
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_DURABILITY.getKey(), "sync");
    // write with no durability
    BatchWriterConfig cfg = new BatchWriterConfig();
    cfg.setDurability(Durability.NONE);
    writeSome(tableName, 10, cfg);
    // verify writes are lost on restart
    restartTServer();
    assertTrue(10 > count(tableName));
  }
  
  private int count(String tableName) throws Exception {
    return FunctionalTestUtils.count(getConnector().createScanner(tableName, Authorizations.EMPTY));
  }

  private void writeSome(String tableName, int n, BatchWriterConfig cfg) throws Exception {
    Connector c = getConnector();
    BatchWriter bw = c.createBatchWriter(tableName, cfg);
    for (int i = 0; i < 10; i++) {
      Mutation m = new Mutation(i + "");
      m.put("", "", "");
      bw.addMutation(m);
    }
    bw.close();
  }
  
  private void restartTServer() throws Exception {
    for (ProcessReference proc : cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
      cluster.killProcess(ServerType.TABLET_SERVER, proc);
    }
    cluster.start();
  }

}
