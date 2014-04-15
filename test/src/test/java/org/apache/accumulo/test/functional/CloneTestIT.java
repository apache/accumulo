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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.DiskUsage;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class CloneTestIT extends SimpleMacIT {

  @Override
  protected int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @Test
  public void testProps() throws Exception {
    String[] tableNames = getUniqueNames(2);
    String table1 = tableNames[0];
    String table2 = tableNames[1];

    Connector c = getConnector();

    c.tableOperations().create(table1);

    c.tableOperations().setProperty(table1, Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "1M");
    c.tableOperations().setProperty(table1, Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX.getKey(), "2M");
    c.tableOperations().setProperty(table1, Property.TABLE_FILE_MAX.getKey(), "23");

    BatchWriter bw = writeData(table1, c);

    Map<String,String> props = new HashMap<String,String>();
    props.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "500K");

    Set<String> exclude = new HashSet<String>();
    exclude.add(Property.TABLE_FILE_MAX.getKey());

    c.tableOperations().clone(table1, table2, true, props, exclude);

    Mutation m3 = new Mutation("009");
    m3.put("data", "x", "1");
    m3.put("data", "y", "2");
    bw.addMutation(m3);
    bw.close();

    checkData(table2, c);

    HashMap<String,String> tableProps = new HashMap<String,String>();
    for (Entry<String,String> prop : c.tableOperations().getProperties(table2)) {
      tableProps.put(prop.getKey(), prop.getValue());
    }

    Assert.assertEquals("500K", tableProps.get(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey()));
    Assert.assertEquals(Property.TABLE_FILE_MAX.getDefaultValue(), tableProps.get(Property.TABLE_FILE_MAX.getKey()));
    Assert.assertEquals("2M", tableProps.get(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX.getKey()));

    c.tableOperations().delete(table1);
    c.tableOperations().delete(table2);

  }

  private void checkData(String table2, Connector c) throws TableNotFoundException {
    Scanner scanner = c.createScanner(table2, Authorizations.EMPTY);

    HashMap<String,String> expected = new HashMap<String,String>();
    expected.put("001:x", "9");
    expected.put("001:y", "7");
    expected.put("008:x", "3");
    expected.put("008:y", "4");

    HashMap<String,String> actual = new HashMap<String,String>();

    for (Entry<Key,Value> entry : scanner)
      actual.put(entry.getKey().getRowData().toString() + ":" + entry.getKey().getColumnQualifierData().toString(), entry.getValue().toString());

    Assert.assertEquals(expected, actual);
  }

  private BatchWriter writeData(String table1, Connector c) throws TableNotFoundException, MutationsRejectedException {
    BatchWriter bw = c.createBatchWriter(table1, new BatchWriterConfig());

    Mutation m1 = new Mutation("001");
    m1.put("data", "x", "9");
    m1.put("data", "y", "7");

    Mutation m2 = new Mutation("008");
    m2.put("data", "x", "3");
    m2.put("data", "y", "4");

    bw.addMutation(m1);
    bw.addMutation(m2);

    bw.flush();
    return bw;
  }

  @Test
  public void testDeleteClone() throws Exception {
    String[] tableNames = getUniqueNames(3);
    String table1 = tableNames[0];
    String table2 = tableNames[1];
    String table3 = tableNames[2];

    Connector c = getConnector();

    // verify that deleting a new table removes the files
    c.tableOperations().create(table3);
    writeData(table3, c).close();
    c.tableOperations().flush(table3, null, null, true);
    // check for files
    FileSystem fs = FileSystem.get(new Configuration());
    String id = c.tableOperations().tableIdMap().get(table3);
    FileStatus[] status = fs.listStatus(new Path(rootPath() + "/accumulo/tables/" + id));
    assertTrue(status.length > 0);
    // verify disk usage
    List<DiskUsage> diskUsage = c.tableOperations().getDiskUsage(Collections.singleton(table3));
    assertEquals(1, diskUsage.size());
    assertTrue(diskUsage.get(0).getUsage() > 100);
    // delete the table
    c.tableOperations().delete(table3);
    // verify its gone from the file system
    Path tablePath = new Path(rootPath() + "/accumulo/tables/" + id);
    if (fs.exists(tablePath)) {
      status = fs.listStatus(tablePath);
      assertTrue(status == null || status.length == 0);
    }

    c.tableOperations().create(table1);

    BatchWriter bw = writeData(table1, c);

    Map<String,String> props = new HashMap<String,String>();
    props.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "500K");

    Set<String> exclude = new HashSet<String>();
    exclude.add(Property.TABLE_FILE_MAX.getKey());

    c.tableOperations().clone(table1, table2, true, props, exclude);

    Mutation m3 = new Mutation("009");
    m3.put("data", "x", "1");
    m3.put("data", "y", "2");
    bw.addMutation(m3);
    bw.close();

    // delete source table, should not affect clone
    c.tableOperations().delete(table1);

    checkData(table2, c);

    c.tableOperations().compact(table2, null, null, true, true);

    checkData(table2, c);

    c.tableOperations().delete(table2);

  }

}
