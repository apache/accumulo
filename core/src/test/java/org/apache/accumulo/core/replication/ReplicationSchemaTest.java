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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.replication.ReplicationSchema.ReplicationSection;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class ReplicationSchemaTest {

  @Test
  public void extractFile() {
    String file = "hdfs://foo:8020/bar";
    Key k = new Key(ReplicationSection.getRowPrefix() + file);
    Text extractedFile = new Text();
    ReplicationSection.getFile(k, extractedFile);
    Assert.assertEquals(file, extractedFile.toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void incorrectPrefixOnFileExtract() {
    String file = "hdfs://foo:8020/bar";
    Key k = new Key("somethingelse" + file);
    Text extractedFile = new Text();
    ReplicationSection.getFile(k, extractedFile);
  }

  @Test(expected = NullPointerException.class)
  public void failOnNullKeyForFileExtract() {
    Text extractedFile = new Text();
    ReplicationSection.getFile(null, extractedFile);
  }

  @Test(expected = NullPointerException.class)
  public void failOnNullBufferForFileExtract() {
    String file = "hdfs://foo:8020/bar";
    Key k = new Key(ReplicationSection.getRowPrefix() + file);
    Text extractedFile = null;
    ReplicationSection.getFile(k, extractedFile);
  }

  @Test(expected = IllegalArgumentException.class)
  public void failOnExtractEmptyFile() {
    String file = "";
    Key k = new Key(ReplicationSection.getRowPrefix() + file);
    Text extractedFile = new Text();
    ReplicationSection.getFile(k, extractedFile);
    Assert.assertEquals(file, extractedFile.toString());
  }

  @Test
  public void extractTableId() {
    String tableId = "1";
    Key k = new Key(ReplicationSection.getRowPrefix() + "foo", tableId);
    Assert.assertEquals(tableId, ReplicationSection.getTableId(k));
  }

  @Test
  public void extractTableIdUsingText() {
    String tableId = "1";
    Key k = new Key(ReplicationSection.getRowPrefix() + "foo", tableId);
    Text buffer = new Text();
    ReplicationSection.getTableId(k, buffer);
    Assert.assertEquals(tableId, buffer.toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void incorrectPrefixOnTableIdExtract() {
    String tableId = "1";
    String file = "hdfs://foo:8020/bar";
    Key k = new Key("somethingelse" + file, tableId);
    Text extractedFile = new Text();
    ReplicationSection.getFile(k, extractedFile);
  }

  @Test(expected = NullPointerException.class)
  public void failOnNullKeyForTableIdExtract() {
    Text extractedFile = new Text();
    ReplicationSection.getFile(null, extractedFile);
  }

  @Test(expected = NullPointerException.class)
  public void failOnNullBufferForTableIdExtract() {
    String file = "hdfs://foo:8020/bar";
    Key k = new Key(ReplicationSection.getRowPrefix() + file);
    Text extractedFile = null;
    ReplicationSection.getFile(k, extractedFile);
  }
}
