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
package org.apache.accumulo.hadoop.its.mapreduce;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class MapReduceIT extends ConfigurableMacBase {

  public static final String hadoopTmpDirArg =
      "-Dhadoop.tmp.dir=" + System.getProperty("user.dir") + "/target/hadoop-tmp";

  static final String tablename = "mapredf";
  static final String input_cf = "cf-HASHTYPE";
  static final String input_cq = "cq-NOTHASHED";
  static final String input_cfcq = input_cf + ":" + input_cq;
  static final String output_cq = "cq-MD4BASE64";

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Test
  public void test() throws Exception {
    var props = getClientProperties();
    try (AccumuloClient client = Accumulo.newClient().from(props).build()) {
      runTest(client, getCluster());
    }
  }

  @SuppressFBWarnings(value = "WEAK_MESSAGE_DIGEST_MD5", justification = "md5 is okay for testing")
  public static void runTest(AccumuloClient c, MiniAccumuloClusterImpl cluster)
      throws AccumuloException, AccumuloSecurityException, TableExistsException,
      TableNotFoundException, MutationsRejectedException, IOException, InterruptedException,
      NoSuchAlgorithmException {
    c.tableOperations().create(tablename);
    BatchWriter bw = c.createBatchWriter(tablename);
    for (int i = 0; i < 10; i++) {
      Mutation m = new Mutation("" + i);
      m.put(input_cf, input_cq, "row" + i);
      bw.addMutation(m);
    }
    bw.close();

    Process hash = cluster.exec(RowHashIT.RowHash.class, Collections.singletonList(hadoopTmpDirArg),
        "-c", cluster.getClientPropsPath(), "-t", tablename, "--column", input_cfcq).getProcess();
    assertEquals(0, hash.waitFor());

    try (Scanner s = c.createScanner(tablename, Authorizations.EMPTY)) {
      s.fetchColumn(new Text(input_cf), new Text(output_cq));
      int i = 0;
      for (Entry<Key,Value> entry : s) {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] check = Base64.getEncoder().encode(md.digest(("row" + i).getBytes()));
        assertEquals(entry.getValue().toString(), new String(check));
        i++;
      }
    }
  }
}
