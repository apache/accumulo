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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.collect.Iterables;

public class BadLocalityGroupMincIT extends AccumuloClusterHarness {

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();

    String tableName = getUniqueNames(1)[0];
    Map<String,String> props = new HashMap<>();

    // intentionally bad locality group config where two groups share a family
    props.put(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + "g1", "fam1,fam2");
    props.put(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + "g2", "fam2,fam3");
    props.put(Property.TABLE_LOCALITY_GROUPS.getKey(), "g1,g2");

    c.tableOperations().create(tableName, new NewTableConfiguration().setProperties(props));

    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m = new Mutation(new Text("r1"));
    m.put(new Text("acf"), new Text(tableName), new Value("1".getBytes(UTF_8)));

    bw.addMutation(m);
    bw.close();

    FunctionalTestUtils.checkRFiles(c, tableName, 1, 1, 0, 0);

    // even with bad locality group config, the minor compaction should still work
    c.tableOperations().flush(tableName, null, null, true);

    FunctionalTestUtils.checkRFiles(c, tableName, 1, 1, 1, 1);

    Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY);
    Entry<Key,Value> entry = Iterables.getOnlyElement(scanner);

    assertEquals("r1", entry.getKey().getRowData().toString());
    assertEquals("acf", entry.getKey().getColumnFamilyData().toString());
    assertEquals(tableName, entry.getKey().getColumnQualifierData().toString());
    assertEquals("1", entry.getValue().toString());

    // this should not hang
    c.tableOperations().delete(tableName);
  }

}
