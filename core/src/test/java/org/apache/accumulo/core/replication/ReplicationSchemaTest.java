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
package org.apache.accumulo.core.replication;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.replication.ReplicationSchema.OrderSection;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class ReplicationSchemaTest {

  @Test
  public void extractFile() {
    String file = "hdfs://foo:8020/bar";
    Key k = new Key(new Text(file), StatusSection.NAME);
    Text extractedFile = new Text();
    StatusSection.getFile(k, extractedFile);
    Assert.assertEquals(file, extractedFile.toString());
  }

  @Test(expected = NullPointerException.class)
  public void failOnNullKeyForFileExtract() {
    Text extractedFile = new Text();
    StatusSection.getFile(null, extractedFile);
  }

  @Test(expected = NullPointerException.class)
  public void failOnNullBufferForFileExtract() {
    String file = "hdfs://foo:8020/bar";
    Key k = new Key(file);
    Text extractedFile = null;
    StatusSection.getFile(k, extractedFile);
  }

  @Test(expected = IllegalArgumentException.class)
  public void failOnExtractEmptyFile() {
    String file = "";
    Key k = new Key(file);
    Text extractedFile = new Text();
    StatusSection.getFile(k, extractedFile);
    Assert.assertEquals(file, extractedFile.toString());
  }

  @Test
  public void extractTableId() {
    Table.ID tableId = Table.ID.of("1");
    Key k = new Key(new Text("foo"), StatusSection.NAME, new Text(tableId.getUtf8()));
    Assert.assertEquals(tableId, StatusSection.getTableId(k));
  }

  @Test
  public void extractTableIdUsingText() {
    Table.ID tableId = Table.ID.of("1");
    Key k = new Key(new Text("foo"), StatusSection.NAME, new Text(tableId.getUtf8()));
    Assert.assertEquals(tableId, StatusSection.getTableId(k));
  }

  @Test(expected = NullPointerException.class)
  public void failOnNullKeyForTableIdExtract() {
    Text extractedFile = new Text();
    StatusSection.getFile(null, extractedFile);
  }

  @Test(expected = NullPointerException.class)
  public void failOnNullBufferForTableIdExtract() {
    String file = "hdfs://foo:8020/bar";
    Key k = new Key(file);
    Text extractedFile = null;
    StatusSection.getFile(k, extractedFile);
  }

  @Test(expected = IllegalArgumentException.class)
  public void failOnIncorrectStatusColfam() {
    Key k = new Key("file", WorkSection.NAME.toString(), "");
    StatusSection.getFile(k, new Text());
  }

  @Test(expected = IllegalArgumentException.class)
  public void failOnIncorrectWorkColfam() {
    Key k = new Key("file", StatusSection.NAME.toString(), "");
    WorkSection.getFile(k, new Text());
  }

  @Test
  public void orderSerialization() {
    long now = System.currentTimeMillis();
    Mutation m = OrderSection.createMutation("/accumulo/file", now);
    Key k = new Key(new Text(m.getRow()));
    Assert.assertEquals("/accumulo/file", OrderSection.getFile(k));
    Assert.assertEquals(now, OrderSection.getTimeClosed(k));
  }

  @Test
  public void orderSerializationWithBuffer() {
    Text buff = new Text();
    long now = System.currentTimeMillis();
    Mutation m = OrderSection.createMutation("/accumulo/file", now);
    Key k = new Key(new Text(m.getRow()));
    Assert.assertEquals("/accumulo/file", OrderSection.getFile(k, buff));
    Assert.assertEquals(now, OrderSection.getTimeClosed(k, buff));
  }

  @Test
  public void separatorDoesntInterferWithSplit() {
    Text buff = new Text();
    // Cycle through 2*128 values
    for (long i = 1; i < 258; i++) {
      Mutation m = OrderSection.createMutation("/accumulo/file", i);
      Key k = new Key(new Text(m.getRow()));
      Assert.assertEquals("/accumulo/file", OrderSection.getFile(k, buff));
      Assert.assertEquals(i, OrderSection.getTimeClosed(k, buff));
    }
  }
}
