package org.apache.accumulo.test.functional;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;

public class AccumuloInputFormatIT extends SimpleMacIT {

  AccumuloInputFormat inputFormat;

  @Before
  public void before() {
    inputFormat = new AccumuloInputFormat();
  }

  /**
   * Tests several different paths through the getSplits() method by setting different properties and verifying the results.
   */
  @Test
  public void testGetSplits() throws IOException, AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
    String table = getTableNames(1)[0];
    getConnector().tableOperations().create(table);
    insertData(table, currentTimeMillis());

    Job job = new Job();
    AccumuloInputFormat.setInputTableName(job, table);
    AccumuloInputFormat.setZooKeeperInstance(job, getConnector().getInstance().getInstanceName(), getConnector().getInstance().getZooKeepers());
    AccumuloInputFormat.setConnectorInfo(job, "root", new PasswordToken(ROOT_PASSWORD));

    // split table
    TreeSet<Text> splitsToAdd = new TreeSet<Text>();
    for (int i = 0; i < 10000; i += 1000)
      splitsToAdd.add(new Text(String.format("%09d", i)));
    getConnector().tableOperations().addSplits(table, splitsToAdd);
    UtilWaitThread.sleep(500); // wait for splits to be propagated

    // get splits without setting any range
    Collection<Text> actualSplits = getConnector().tableOperations().listSplits(table);
    List<InputSplit> splits = inputFormat.getSplits(job);
    assertEquals(actualSplits.size() + 1, splits.size()); // No ranges set on the job so it'll start with -inf

    // set ranges and get splits
    List<Range> ranges = new ArrayList<Range>();
    for (Text text : actualSplits)
      ranges.add(new Range(text));
    AccumuloInputFormat.setRanges(job, ranges);
    splits = inputFormat.getSplits(job);
    assertEquals(actualSplits.size(), splits.size());

    // offline mode
    AccumuloInputFormat.setOfflineTableScan(job, true);
    try {
      inputFormat.getSplits(job);
      fail("An exception should have been thrown");
    } catch (Exception e) {}

    getConnector().tableOperations().offline(table);
    splits = inputFormat.getSplits(job);
    assertEquals(actualSplits.size(), splits.size());

    // auto adjust ranges
    ranges = new ArrayList<Range>();
    for (int i = 0; i < 5; i++) // overlapping ranges
      ranges.add(new Range(String.format("%09d", i), String.format("%09d", i + 2)));
    AccumuloInputFormat.setRanges(job, ranges);
    splits = inputFormat.getSplits(job);
    assertEquals(2, splits.size());

    AccumuloInputFormat.setAutoAdjustRanges(job, false);
    splits = inputFormat.getSplits(job);
    assertEquals(ranges.size(), splits.size());
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
