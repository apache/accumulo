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
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;

public class AccumuloInputFormatIT extends AccumuloClusterIT {

  AccumuloInputFormat inputFormat;

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Before
  public void before() {
    inputFormat = new AccumuloInputFormat();
  }

  /**
   * Tests several different paths through the getSplits() method by setting different properties and verifying the results.
   */
  @Test
  public void testGetSplits() throws IOException, AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
    Connector conn = getConnector();
    String table = getUniqueNames(1)[0];
    conn.tableOperations().create(table);
    insertData(table, currentTimeMillis());

    ClientConfiguration clientConf = new ClientConfiguration().withInstance(conn.getInstance().getInstanceName()).withZkHosts(
        conn.getInstance().getZooKeepers());
    AccumuloConfiguration clusterClientConf = new ConfigurationCopy(new DefaultConfiguration());

    // Pass SSL and CredentialProvider options into the ClientConfiguration given to AccumuloInputFormat
    boolean sslEnabled = Boolean.valueOf(clusterClientConf.get(Property.INSTANCE_RPC_SSL_ENABLED));
    if (sslEnabled) {
      ClientProperty[] sslProperties = new ClientProperty[] {ClientProperty.INSTANCE_RPC_SSL_ENABLED, ClientProperty.INSTANCE_RPC_SSL_CLIENT_AUTH,
          ClientProperty.RPC_SSL_KEYSTORE_PATH, ClientProperty.RPC_SSL_KEYSTORE_TYPE, ClientProperty.RPC_SSL_KEYSTORE_PASSWORD,
          ClientProperty.RPC_SSL_TRUSTSTORE_PATH, ClientProperty.RPC_SSL_TRUSTSTORE_TYPE, ClientProperty.RPC_SSL_TRUSTSTORE_PASSWORD,
          ClientProperty.RPC_USE_JSSE, ClientProperty.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS};

      for (ClientProperty prop : sslProperties) {
        // The default property is returned if it's not in the ClientConfiguration so we don't have to check if the value is actually defined
        clientConf.setProperty(prop, clusterClientConf.get(prop.getAccumuloProperty()));
      }
    }

    @SuppressWarnings("deprecation")
    Job job = new Job();
    AccumuloInputFormat.setInputTableName(job, table);
    AccumuloInputFormat.setZooKeeperInstance(job, clientConf);
    AccumuloInputFormat.setConnectorInfo(job, getPrincipal(), getToken());

    // split table
    TreeSet<Text> splitsToAdd = new TreeSet<Text>();
    for (int i = 0; i < 10000; i += 1000)
      splitsToAdd.add(new Text(String.format("%09d", i)));
    conn.tableOperations().addSplits(table, splitsToAdd);
    UtilWaitThread.sleep(500); // wait for splits to be propagated

    // get splits without setting any range
    Collection<Text> actualSplits = conn.tableOperations().listSplits(table);
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

    conn.tableOperations().offline(table);
    splits = inputFormat.getSplits(job);
    assertEquals(actualSplits.size(), splits.size());

    // auto adjust ranges
    ranges = new ArrayList<Range>();
    for (int i = 0; i < 5; i++)
      // overlapping ranges
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
