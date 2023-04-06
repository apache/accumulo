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
package org.apache.accumulo.server.constraints;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.zookeeper.TransactionWatcher.Arbitrator;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class MetadataConstraintsTest {

  static class TestMetadataConstraints extends MetadataConstraints {
    @Override
    protected Arbitrator getArbitrator(ServerContext context) {
      return new Arbitrator() {

        @Override
        public boolean transactionAlive(String type, long tid) {
          if (tid == 9) {
            throw new RuntimeException("txid 9 reserved for future use");
          }
          return tid == 5 || tid == 7;
        }

        @Override
        public boolean transactionComplete(String type, long tid) {
          return tid != 5 && tid != 7;
        }
      };
    }
  }

  private SystemEnvironment createEnv() {
    SystemEnvironment env = EasyMock.createMock(SystemEnvironment.class);
    ServerContext context = EasyMock.createMock(ServerContext.class);
    EasyMock.expect(env.getServerContext()).andReturn(context);
    EasyMock.replay(env);
    return env;
  }

  @Test
  public void testCheck() {
    Mutation m = new Mutation(new Text("0;foo"));
    TabletColumnFamily.PREV_ROW_COLUMN.put(m, new Value("1foo"));

    MetadataConstraints mc = new MetadataConstraints();

    List<Short> violations = mc.check(createEnv(), m);

    assertNotNull(violations);
    assertEquals(1, violations.size());
    assertEquals(Short.valueOf((short) 3), violations.get(0));

    m = new Mutation(new Text("0:foo"));
    TabletColumnFamily.PREV_ROW_COLUMN.put(m, new Value("1poo"));

    violations = mc.check(createEnv(), m);

    assertNotNull(violations);
    assertEquals(1, violations.size());
    assertEquals(Short.valueOf((short) 4), violations.get(0));

    m = new Mutation(new Text("0;foo"));
    m.put("bad_column_name", "", "e");

    violations = mc.check(createEnv(), m);

    assertNotNull(violations);
    assertEquals(1, violations.size());
    assertEquals(Short.valueOf((short) 2), violations.get(0));

    m = new Mutation(new Text("!!<"));
    TabletColumnFamily.PREV_ROW_COLUMN.put(m, new Value("1poo"));

    violations = mc.check(createEnv(), m);

    assertNotNull(violations);
    assertEquals(2, violations.size());
    assertEquals(Short.valueOf((short) 4), violations.get(0));
    assertEquals(Short.valueOf((short) 5), violations.get(1));

    m = new Mutation(new Text("0;foo"));
    TabletColumnFamily.PREV_ROW_COLUMN.put(m, new Value(""));

    violations = mc.check(createEnv(), m);

    assertNotNull(violations);
    assertEquals(1, violations.size());
    assertEquals(Short.valueOf((short) 6), violations.get(0));

    m = new Mutation(new Text("0;foo"));
    TabletColumnFamily.PREV_ROW_COLUMN.put(m, new Value("bar"));

    violations = mc.check(createEnv(), m);

    assertNull(violations);

    m = new Mutation(new Text(MetadataTable.ID.canonical() + "<"));
    TabletColumnFamily.PREV_ROW_COLUMN.put(m, new Value("bar"));

    violations = mc.check(createEnv(), m);

    assertNull(violations);

    m = new Mutation(new Text("!1<"));
    TabletColumnFamily.PREV_ROW_COLUMN.put(m, new Value("bar"));

    violations = mc.check(createEnv(), m);

    assertNotNull(violations);
    assertEquals(1, violations.size());
    assertEquals(Short.valueOf((short) 4), violations.get(0));

  }

  @Test
  public void testBulkFileCheck() {
    MetadataConstraints mc = new TestMetadataConstraints();
    Mutation m;
    List<Short> violations;

    // inactive txid
    m = new Mutation(new Text("0;foo"));
    m.put(BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("12345"));
    m.put(DataFileColumnFamily.NAME, new Text("/someFile"),
        new DataFileValue(1, 1).encodeAsValue());
    violations = mc.check(createEnv(), m);
    assertNotNull(violations);
    assertEquals(1, violations.size());
    assertEquals(Short.valueOf((short) 8), violations.get(0));

    // txid that throws exception
    m = new Mutation(new Text("0;foo"));
    m.put(BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("9"));
    m.put(DataFileColumnFamily.NAME, new Text("/someFile"),
        new DataFileValue(1, 1).encodeAsValue());
    violations = mc.check(createEnv(), m);
    assertNotNull(violations);
    assertEquals(1, violations.size());
    assertEquals(Short.valueOf((short) 8), violations.get(0));

    // active txid w/ file
    m = new Mutation(new Text("0;foo"));
    m.put(BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("5"));
    m.put(DataFileColumnFamily.NAME, new Text("/someFile"),
        new DataFileValue(1, 1).encodeAsValue());
    violations = mc.check(createEnv(), m);
    assertNull(violations);

    // active txid w/o file
    m = new Mutation(new Text("0;foo"));
    m.put(BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("5"));
    violations = mc.check(createEnv(), m);
    assertNotNull(violations);
    assertEquals(1, violations.size());
    assertEquals(Short.valueOf((short) 8), violations.get(0));

    // two active txids w/ files
    m = new Mutation(new Text("0;foo"));
    m.put(BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("5"));
    m.put(DataFileColumnFamily.NAME, new Text("/someFile"),
        new DataFileValue(1, 1).encodeAsValue());
    m.put(BulkFileColumnFamily.NAME, new Text("/someFile2"), new Value("7"));
    m.put(DataFileColumnFamily.NAME, new Text("/someFile2"),
        new DataFileValue(1, 1).encodeAsValue());
    violations = mc.check(createEnv(), m);
    assertNotNull(violations);
    assertEquals(1, violations.size());
    assertEquals(Short.valueOf((short) 8), violations.get(0));

    // two files w/ one active txid
    m = new Mutation(new Text("0;foo"));
    m.put(BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("5"));
    m.put(DataFileColumnFamily.NAME, new Text("/someFile"),
        new DataFileValue(1, 1).encodeAsValue());
    m.put(BulkFileColumnFamily.NAME, new Text("/someFile2"), new Value("5"));
    m.put(DataFileColumnFamily.NAME, new Text("/someFile2"),
        new DataFileValue(1, 1).encodeAsValue());
    violations = mc.check(createEnv(), m);
    assertNull(violations);

    // two loaded w/ one active txid and one file
    m = new Mutation(new Text("0;foo"));
    m.put(BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("5"));
    m.put(DataFileColumnFamily.NAME, new Text("/someFile"),
        new DataFileValue(1, 1).encodeAsValue());
    m.put(BulkFileColumnFamily.NAME, new Text("/someFile2"), new Value("5"));
    violations = mc.check(createEnv(), m);
    assertNotNull(violations);
    assertEquals(1, violations.size());
    assertEquals(Short.valueOf((short) 8), violations.get(0));

    // active txid, mutation that looks like split
    m = new Mutation(new Text("0;foo"));
    m.put(BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("5"));
    ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value("/t1"));
    violations = mc.check(createEnv(), m);
    assertNull(violations);

    // inactive txid, mutation that looks like split
    m = new Mutation(new Text("0;foo"));
    m.put(BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("12345"));
    ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value("/t1"));
    violations = mc.check(createEnv(), m);
    assertNull(violations);

    // active txid, mutation that looks like a load
    m = new Mutation(new Text("0;foo"));
    m.put(BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("5"));
    m.put(CurrentLocationColumnFamily.NAME, new Text("789"), new Value("127.0.0.1:9997"));
    violations = mc.check(createEnv(), m);
    assertNull(violations);

    // inactive txid, mutation that looks like a load
    m = new Mutation(new Text("0;foo"));
    m.put(BulkFileColumnFamily.NAME, new Text("/someFile"), new Value("12345"));
    m.put(CurrentLocationColumnFamily.NAME, new Text("789"), new Value("127.0.0.1:9997"));
    violations = mc.check(createEnv(), m);
    assertNull(violations);

    // deleting a load flag
    m = new Mutation(new Text("0;foo"));
    m.putDelete(BulkFileColumnFamily.NAME, new Text("/someFile"));
    violations = mc.check(createEnv(), m);
    assertNull(violations);

  }

}
