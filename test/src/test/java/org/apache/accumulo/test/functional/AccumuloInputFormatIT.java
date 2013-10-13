package org.apache.accumulo.test.functional;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

public class AccumuloInputFormatIT extends SimpleMacIT {

  @Test
  public void testGetSplits() throws IOException, AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {

    String table = getTableNames(1)[0];
    getConnector().tableOperations().create(table);

    // add data

    // add splits

    Job job = new Job();

    // set up the job here
    AccumuloInputFormat.setInputTableName(job, table);
    AccumuloInputFormat.setZooKeeperInstance(job, getConnector().getInstance().getInstanceName(), getConnector().getInstance().getZooKeepers());
    AccumuloInputFormat.setConnectorInfo(job, "root", new PasswordToken(ROOT_PASSWORD));

    AccumuloInputFormat inputFormat = new AccumuloInputFormat();

    insertData(table, currentTimeMillis());

    TreeSet<Text> splitsToAdd = new TreeSet<Text>();
    for (int i = 0; i < 10000; i += 1000)
      splitsToAdd.add(new Text(String.format("%09d", i)));

    getConnector().tableOperations().addSplits(table, splitsToAdd);

    UtilWaitThread.sleep(5000);
    Collection<Text> actualSplits = getConnector().tableOperations().listSplits(table);

    List<InputSplit> splits = inputFormat.getSplits(job);
    assertEquals(actualSplits.size(), splits.size());

  }

  private void insertData(String tableName, long ts) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    BatchWriter bw = getConnector().createBatchWriter(tableName, null);

    for (int i = 0; i < 10000; i++) {
      String row = String.format("%09d", i);

      Mutation m = new Mutation(new Text(row));
      m.put(new Text("cf1"), new Text("cq1"), ts, new Value(("" + i).getBytes()));
      bw.addMutation(m);
    }
    bw.close();
  }
}
