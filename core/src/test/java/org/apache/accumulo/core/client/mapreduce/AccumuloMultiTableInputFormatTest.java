package org.apache.accumulo.core.client.mapreduce;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AccumuloMultiTableInputFormatTest {

  private static final String PREFIX = AccumuloMultiTableInputFormatTest.class.getSimpleName();
  private static final String INSTANCE_NAME = PREFIX + "_mapreduce_instance";
  private static final String TEST_TABLE_1 = PREFIX + "_mapreduce_table_1";
  private static final String TEST_TABLE_2 = PREFIX + "_mapreduce_table_2";
  
  private static AssertionError e1 = null;
  private static AssertionError e2 = null;
  
  private static class MRTester extends Configured implements Tool {
    
    private static class TestMapper extends Mapper<Key,Value,Key,Value> {
      Key key = null;
      int count = 0;
      
      @Override
      protected void map(Key k, Value v, Context context) throws IOException, InterruptedException {
        try {
          String tableName = ((InputFormatBase.RangeInputSplit) context.getInputSplit()).getTableName();
          if (key != null)
            assertEquals(key.getRow().toString(), new String(v.get()));
          assertEquals(new Text(String.format("%s_%09x", tableName, count + 1)), k.getRow());
          assertEquals(String.format("%s_%09x", tableName, count), new String(v.get()));
        } catch (AssertionError e) {
          e1 = e;
        }
        key = new Key(k);
        count++;
      }
      
      @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
        try {
          assertEquals(100, count);
        } catch (AssertionError e) {
          e2 = e;
        }
      }
    }
    
    @Override
    public int run(String[] args) throws Exception {
      
      if (args.length != 4) {
        throw new IllegalArgumentException("Usage : " + MRTester.class.getName() + " <user> <pass> <table1> <table2>");
      }
      
      String user = args[0];
      String pass = args[1];
      String table1 = args[2];
      String table2 = args[3];
      
      Job job = new Job(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
      job.setJarByClass(this.getClass());
      
      job.setInputFormatClass(AccumuloMultiTableInputFormat.class);
      
      AccumuloMultiTableInputFormat.setConnectorInfo(job, user, new PasswordToken(pass));
      
      BatchScanConfig tableConfig1 = new BatchScanConfig(table1);
      BatchScanConfig tableConfig2 = new BatchScanConfig(table2);
      
      AccumuloMultiTableInputFormat.setBatchScanConfigs(job, tableConfig1, tableConfig2);
      AccumuloMultiTableInputFormat.setMockInstance(job, INSTANCE_NAME);
      
      job.setMapperClass(TestMapper.class);
      job.setMapOutputKeyClass(Key.class);
      job.setMapOutputValueClass(Value.class);
      job.setOutputFormatClass(NullOutputFormat.class);
      
      job.setNumReduceTasks(0);
      
      job.waitForCompletion(true);
      
      return job.isSuccessful() ? 0 : 1;
    }
    
    public static void main(String[] args) throws Exception {
      assertEquals(0, ToolRunner.run(CachedConfiguration.getInstance(), new MRTester(), args));
    }
  }
  
  /**
   * Generate incrementing counts and attach table name to the key/value so that order and multi-table data can be verified.
   */
  @Test
  public void testMap() throws Exception {
    MockInstance mockInstance = new MockInstance(INSTANCE_NAME);
    Connector c = mockInstance.getConnector("root", new PasswordToken(""));
    c.tableOperations().create(TEST_TABLE_1);
    c.tableOperations().create(TEST_TABLE_2);
    BatchWriter bw = c.createBatchWriter(TEST_TABLE_1, new BatchWriterConfig());
    BatchWriter bw2 = c.createBatchWriter(TEST_TABLE_2, new BatchWriterConfig());
    for (int i = 0; i < 100; i++) {
      Mutation t1m = new Mutation(new Text(String.format("%s_%09x", TEST_TABLE_1, i + 1)));
      t1m.put(new Text(), new Text(), new Value(String.format("%s_%09x", TEST_TABLE_1, i).getBytes()));
      bw.addMutation(t1m);
      Mutation t2m = new Mutation(new Text(String.format("%s_%09x", TEST_TABLE_2, i + 1)));
      t2m.put(new Text(), new Text(), new Value(String.format("%s_%09x", TEST_TABLE_2, i).getBytes()));
      bw2.addMutation(t2m);
    }
    bw.close();
    bw2.close();
    
    MRTester.main(new String[] {"root", "", TEST_TABLE_1, TEST_TABLE_2});
    assertNull(e1);
    assertNull(e2);
  }
  
  /**
   * Verify {@link BatchScanConfig} objects get correctly serialized in the JobContext.
   */
  @Test
  public void testTableQueryConfigSerialization() throws IOException {
    
    Job job = new Job();
    
    BatchScanConfig table1 = new BatchScanConfig(TEST_TABLE_1).setRanges(Collections.singletonList(new Range("a", "b")))
        .fetchColumns(Collections.singleton(new Pair<Text, Text>(new Text("CF1"), new Text("CQ1"))))
        .setIterators(Collections.singletonList(new IteratorSetting(50, "iter1", "iterclass1")));
    
    BatchScanConfig table2 = new BatchScanConfig(TEST_TABLE_2).setRanges(Collections.singletonList(new Range("a", "b")))
        .fetchColumns(Collections.singleton(new Pair<Text, Text>(new Text("CF1"), new Text("CQ1"))))
        .setIterators(Collections.singletonList(new IteratorSetting(50, "iter1", "iterclass1")));
    
    AccumuloMultiTableInputFormat.setBatchScanConfigs(job, table1, table2);
    
    assertEquals(table1, AccumuloMultiTableInputFormat.getBatchScanConfig(job, TEST_TABLE_1));
    assertEquals(table2, AccumuloMultiTableInputFormat.getBatchScanConfig(job, TEST_TABLE_2));
  }
  
}
