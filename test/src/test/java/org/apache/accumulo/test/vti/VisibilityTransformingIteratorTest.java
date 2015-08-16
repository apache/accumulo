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
package org.apache.accumulo.test.vti;

import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.system.VisibilityTransformingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VisibilityTransformingIteratorTest {

  private static final String VTI_TABLE = "vti";
  private static final ColumnVisibility RAW_VIS = new ColumnVisibility("raw");
  private static final Authorizations ALL_AUTHS = new Authorizations(HashingIterator.VIS_HASHED, "raw");
  private static final Authorizations HASHED_AUTHS = new Authorizations(HashingIterator.VIS_HASHED);

  private static MiniAccumuloCluster mac;
  private static Connector rootConn;

  @BeforeClass
  public static void setupMAC() throws Exception {
    Path macPath = Files.createTempDirectory("mac");
    System.out.println("MAC running at " + macPath);
    MiniAccumuloConfig macCfg = new MiniAccumuloConfig(macPath.toFile(), "password");
    macCfg.setNumTservers(1);
    mac = new MiniAccumuloCluster(macCfg);
    mac.start();
    rootConn = mac.getConnector("root", "password");
    rootConn.tableOperations().create(VTI_TABLE);
    rootConn.tableOperations().setProperty("vti", Property.TABLE_VTI_CLASS.getKey(), HashingIterator.class.getName());
    rootConn.securityOperations().changeUserAuthorizations("root",ALL_AUTHS);
  }

  @Test
  public void testVti() throws Exception {
    BatchWriter bw = rootConn.createBatchWriter(VTI_TABLE, new BatchWriterConfig());
    Mutation m = new Mutation("r0");
    m.put("cf0", "cq0", RAW_VIS, new Value("some bytes".getBytes()));
    bw.addMutation(m);
    bw.flush();

    Scanner rawScan = rootConn.createScanner(VTI_TABLE, ALL_AUTHS);
    rawScan.setRange(new Range());
    List<Map.Entry<Key,Value>> allKvs = Lists.newArrayList(rawScan);
    assertTrue("All scan must have 2 kv pairs", allKvs.size() == 2);
    assertTrue("First value must be hashed", HashingIterator.isHashedValue(allKvs.get(0).getValue()));
    assertFalse("Second value must not be hashed", HashingIterator.isHashedValue(allKvs.get(1).getValue()));
    assertTrue("Hashed key must have hashed visibility", HashingIterator.isHashedVisibility(allKvs.get(0).getKey()));
    assertTrue("Raw key must have raw visibility", RAW_VIS.equals(allKvs.get(1).getKey().getColumnVisibilityParsed()));
    rawScan.close();

    rawScan = rootConn.createScanner(VTI_TABLE, HASHED_AUTHS);
    rawScan.setRange(new Range());
    allKvs = Lists.newArrayList(rawScan);
    assertTrue("Hashed scan must have 1 kv pairs", allKvs.size() == 1);
    assertTrue("First value must be hashed", HashingIterator.isHashedValue(allKvs.get(0).getValue()));
    assertTrue("Hashed key must have hashed visibility", HashingIterator.isHashedVisibility(allKvs.get(0).getKey()));
    rawScan.close();
  }

  @Before
  public void clearVtiTable() throws Exception {
    rootConn.tableOperations().deleteRows(VTI_TABLE, null, null);
  }

  @AfterClass
  public static void tearDownMAC() throws Exception {
    mac.stop();
  }

  public static class HashingIterator extends VisibilityTransformingIterator {

    private static final String VIS_HASHED = "hashed";

    private static final byte[] HDR = "sha1:".getBytes();
    private static final HashFunction SHA1 = Hashing.sha1();

    private final byte[] out = new byte[HDR.length + SHA1.bits() / 8];

    public HashingIterator() {
      System.arraycopy(HDR, 0, out, 0, HDR.length);
    }

    @Override
    protected Collection<? extends Map.Entry<ColumnVisibility,Value>> transformVisibility(Key key, Value value) {
      byte[] hash = SHA1.hashBytes(value.get()).asBytes();
      System.arraycopy(hash, 0, out, HDR.length, hash.length);
      return Collections.singletonList(new AbstractMap.SimpleImmutableEntry<>(replaceTerm(key.getColumnVisibilityParsed(), "raw", VIS_HASHED), new Value(out)));
    }

    static boolean isHashedValue(Value v) {
      byte[] b = v.get();
      for (int i = 0; i < HDR.length; i++) {
        if (b[i] != HDR[i]) {
          return false;
        }
      }
      return true;
    }

    static boolean isHashedVisibility(Key k) {
      return VIS_HASHED.equals(k.getColumnVisibility().toString());
    }
  }

}
