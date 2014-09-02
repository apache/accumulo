package org.apache.accumulo.test.functional;

import static org.junit.Assert.*;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
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
  
  @Test
  public void nondurableTableHasDurableWrites() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_DURABILITY.getKey(), "none");
    BatchWriterConfig cfg = new BatchWriterConfig();
    cfg.setDurability(Durability.SYNC);
    write(tableName, 10, cfg);
    assertEquals(10, count(tableName));
    restartTServer();
    assertEquals(10, count(tableName));
  }
  
  @Test
  public void durableTableLosesNonDurableWrites() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_DURABILITY.getKey(), "sync");
    BatchWriterConfig cfg = new BatchWriterConfig();
    cfg.setDurability(Durability.NONE);
    write(tableName, 10, cfg);
    restartTServer();
    assertTrue(10 > count(tableName));
  }
  
  private int count(String tableName) throws Exception {
    Connector c = getConnector();
    Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY);
    int result = 0;
    for (@SuppressWarnings("unused") Entry<Key,Value> entry :scanner) {
      result++;
    }
    return result;
  }

  private void write(String tableName, int n, BatchWriterConfig cfg) throws Exception {
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
