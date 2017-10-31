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
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ReplicationTargetTest {

  @Test
  public void properEquality() {
    ReplicationTarget expected1 = new ReplicationTarget("foo", "bar", Table.ID.of("1"));

    Assert.assertEquals(expected1, new ReplicationTarget("foo", "bar", Table.ID.of("1")));
    Assert.assertNotEquals(expected1, new ReplicationTarget("foo", "foo", Table.ID.of("1")));
    Assert.assertNotEquals(expected1, new ReplicationTarget("bar", "bar", Table.ID.of("1")));
    Assert.assertNotEquals(expected1, new ReplicationTarget(null, "bar", Table.ID.of("1")));
    Assert.assertNotEquals(expected1, new ReplicationTarget("foo", null, Table.ID.of("1")));
  }

  @Test
  public void writableOut() throws Exception {
    ReplicationTarget expected = new ReplicationTarget("foo", "bar", Table.ID.of("1"));
    DataOutputBuffer buffer = new DataOutputBuffer();
    expected.write(buffer);

    DataInputBuffer input = new DataInputBuffer();
    input.reset(buffer.getData(), buffer.getLength());
    ReplicationTarget actual = new ReplicationTarget();
    actual.readFields(input);
  }

  @Test
  public void writableOutWithNulls() throws Exception {
    ReplicationTarget expected = new ReplicationTarget(null, null, null);
    DataOutputBuffer buffer = new DataOutputBuffer();
    expected.write(buffer);

    DataInputBuffer input = new DataInputBuffer();
    input.reset(buffer.getData(), buffer.getLength());
    ReplicationTarget actual = new ReplicationTarget();
    actual.readFields(input);
  }

  @Test
  public void staticFromTextHelper() throws Exception {
    ReplicationTarget expected = new ReplicationTarget("foo", "bar", Table.ID.of("1"));
    DataOutputBuffer buffer = new DataOutputBuffer();
    expected.write(buffer);
    Text t = new Text();
    t.set(buffer.getData(), 0, buffer.getLength());

    Assert.assertEquals(expected, ReplicationTarget.from(t));
  }

  @Test
  public void staticToTextHelper() throws Exception {
    ReplicationTarget expected = new ReplicationTarget("foo", "bar", Table.ID.of("1"));
    DataOutputBuffer buffer = new DataOutputBuffer();
    expected.write(buffer);
    Text t = new Text();
    t.set(buffer.getData(), 0, buffer.getLength());

    Assert.assertEquals(t, expected.toText());
  }

  @Test
  public void staticFromStringHelper() throws Exception {
    ReplicationTarget expected = new ReplicationTarget("foo", "bar", Table.ID.of("1"));
    DataOutputBuffer buffer = new DataOutputBuffer();
    expected.write(buffer);
    Text t = new Text();
    t.set(buffer.getData(), 0, buffer.getLength());

    Assert.assertEquals(expected, ReplicationTarget.from(t.toString()));
  }

}
