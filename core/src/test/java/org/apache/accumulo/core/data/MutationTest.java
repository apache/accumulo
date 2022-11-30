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
package org.apache.accumulo.core.data;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.accumulo.core.dataImpl.thrift.TMutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class MutationTest {

  private static String toHexString(byte[] ba) {
    StringBuilder str = new StringBuilder();
    for (byte b : ba) {
      str.append(String.format("%x", b));
    }
    return str.toString();
  }

  /*
   * Test constructing a Mutation using a byte buffer. The byte array returned as the row is
   * converted to a hexadecimal string for easy comparision.
   */
  @Test
  public void testByteConstructor() {
    Mutation m = new Mutation("0123456789".getBytes());
    assertEquals("30313233343536373839", toHexString(m.getRow()));
  }

  @Test
  public void testLimitedByteConstructor() {
    Mutation m = new Mutation("0123456789".getBytes(), 2, 5);
    assertEquals("3233343536", toHexString(m.getRow()));
  }

  @Test
  public void test1() {
    Mutation m = new Mutation(new Text("r1"));
    m.put(new Text("cf1"), new Text("cq1"), new Value("v1"));

    List<ColumnUpdate> updates = m.getUpdates();

    assertEquals(1, updates.size());

    ColumnUpdate cu = updates.get(0);

    assertEquals("cf1", new String(cu.getColumnFamily()));
    assertEquals("cq1", new String(cu.getColumnQualifier()));
    assertEquals("", new String(cu.getColumnVisibility()));
    assertFalse(cu.hasTimestamp());

  }

  @Test
  public void test2() throws IOException {
    Mutation m = new Mutation(new Text("r1"));
    m.put(new Text("cf1"), new Text("cq1"), new Value("v1"));
    m.put(new Text("cf2"), new Text("cq2"), 56, new Value("v2"));

    List<ColumnUpdate> updates = m.getUpdates();

    assertEquals(2, updates.size());

    assertEquals("r1", new String(m.getRow()));
    ColumnUpdate cu = updates.get(0);

    assertEquals("cf1", new String(cu.getColumnFamily()));
    assertEquals("cq1", new String(cu.getColumnQualifier()));
    assertEquals("", new String(cu.getColumnVisibility()));
    assertFalse(cu.hasTimestamp());

    cu = updates.get(1);

    assertEquals("cf2", new String(cu.getColumnFamily()));
    assertEquals("cq2", new String(cu.getColumnQualifier()));
    assertEquals("", new String(cu.getColumnVisibility()));
    assertTrue(cu.hasTimestamp());
    assertEquals(56, cu.getTimestamp());

    m = cloneMutation(m);

    assertEquals("r1", new String(m.getRow()));
    updates = m.getUpdates();

    assertEquals(2, updates.size());

    cu = updates.get(0);

    assertEquals("cf1", new String(cu.getColumnFamily()));
    assertEquals("cq1", new String(cu.getColumnQualifier()));
    assertEquals("", new String(cu.getColumnVisibility()));
    assertFalse(cu.hasTimestamp());

    cu = updates.get(1);

    assertEquals("cf2", new String(cu.getColumnFamily()));
    assertEquals("cq2", new String(cu.getColumnQualifier()));
    assertEquals("", new String(cu.getColumnVisibility()));
    assertTrue(cu.hasTimestamp());
    assertEquals(56, cu.getTimestamp());

  }

  private Mutation cloneMutation(Mutation m) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    m.write(dos);
    dos.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);

    m = new Mutation();
    m.readFields(dis);
    return m;
  }

  @Test
  public void test3() throws IOException {
    Mutation m = new Mutation(new Text("r1"));
    for (int i = 0; i < 5; i++) {
      int len = Mutation.VALUE_SIZE_COPY_CUTOFF - 2 + i;
      byte[] val = new byte[len];
      for (int j = 0; j < len; j++) {
        val[j] = (byte) i;
      }

      m.put(new Text("cf" + i), new Text("cq" + i), new Value(val));

    }

    for (int r = 0; r < 3; r++) {
      assertEquals("r1", new String(m.getRow()));
      List<ColumnUpdate> updates = m.getUpdates();
      assertEquals(5, updates.size());
      for (int i = 0; i < 5; i++) {
        ColumnUpdate cu = updates.get(i);
        assertEquals("cf" + i, new String(cu.getColumnFamily()));
        assertEquals("cq" + i, new String(cu.getColumnQualifier()));
        assertEquals("", new String(cu.getColumnVisibility()));
        assertFalse(cu.hasTimestamp());

        byte[] val = cu.getValue();
        int len = Mutation.VALUE_SIZE_COPY_CUTOFF - 2 + i;
        assertEquals(len, val.length);
        for (int j = 0; j < len; j++) {
          assertEquals(i, val[j]);
        }
      }

      m = cloneMutation(m);
    }
  }

  private Text nt(String s) {
    return new Text(s);
  }

  private Value nv(String s) {
    return new Value(s);
  }

  @Test
  public void testAtFamilyTypes() {
    final String fam = "f16bc";
    final String qual = "q1pm2";
    final String val = "v8672194923750";

    Mutation expected = new Mutation("row5");
    expected.put(fam, qual, val);

    // Test all family methods, keeping qual and val constant as Strings
    // fam: byte[]
    Mutation actual = new Mutation("row5");
    actual.at().family(fam.getBytes(UTF_8)).qualifier(qual).put(val);
    assertEquals(expected, actual);

    // fam: ByteBuffer
    final ByteBuffer bbFam = ByteBuffer.wrap(fam.getBytes(UTF_8));
    final int bbFamStartPos = bbFam.position();
    actual = new Mutation("row5");
    actual.at().family(bbFam).qualifier(qual).put(val);
    assertEquals(expected, actual);

    // make sure the ByteBuffer last byte filled in the buffer (its position) is same as before the
    // API call
    assertEquals(bbFamStartPos, bbFam.position());

    // fam: CharSequence (String implementation)
    actual = new Mutation("row5");
    actual.at().family(fam).qualifier(qual).put(val);
    assertEquals(expected, actual);

    // fam: Text
    actual = new Mutation("row5");
    actual.at().family(new Text(fam)).qualifier(qual).put(val);
    assertEquals(expected, actual);
  }

  @Test
  public void testAtQualifierTypes() {
    final String fam = "f16bc";
    final String qual = "q1pm2";
    final String val = "v8672194923750";

    Mutation expected = new Mutation("row5");
    expected.put(fam, qual, val);

    // Test all qualifier methods, keeping fam and val constant as Strings
    // qual: byte[]
    Mutation actual = new Mutation("row5");
    actual.at().family(fam).qualifier(qual.getBytes(UTF_8)).put(val);
    assertEquals(expected, actual);

    // qual: ByteBuffer
    final ByteBuffer bbQual = ByteBuffer.wrap(qual.getBytes(UTF_8));
    final int bbQualStartPos = bbQual.position();
    actual = new Mutation("row5");
    actual.at().family(fam).qualifier(bbQual).put(val);
    assertEquals(expected, actual);

    // make sure the ByteBuffer last byte filled in the buffer (its position) is same as before the
    // API call
    assertEquals(bbQualStartPos, bbQual.position());

    // qual: CharSequence (String implementation)
    actual = new Mutation("row5");
    actual.at().family(fam).qualifier(qual).put(val);
    assertEquals(expected, actual);

    // qual: Text
    actual = new Mutation("row5");
    actual.at().family(fam).qualifier(new Text(qual)).put(val);
    assertEquals(expected, actual);
  }

  @Test
  public void testAtVisiblityTypes() {
    final byte[] fam = "f16bc".getBytes(UTF_8);
    final byte[] qual = "q1pm2".getBytes(UTF_8);
    final ColumnVisibility vis = new ColumnVisibility("v35x2");
    final byte[] val = "v8672194923750".getBytes(UTF_8);

    Mutation expected = new Mutation("row5");
    expected.put(fam, qual, vis, val);

    // Test all visibility methods, keeping fam, qual, and val constant as byte arrays
    // vis: byte[]
    Mutation actual = new Mutation("row5");
    actual.at().family(fam).qualifier(qual).visibility(vis.getExpression()).put(val);
    assertEquals(expected, actual);

    // vis: ByteBuffer
    final ByteBuffer bbVis = ByteBuffer.wrap(vis.getExpression());
    final int bbVisStartPos = bbVis.position();
    actual = new Mutation("row5");
    actual.at().family(fam).qualifier(qual).visibility(bbVis).put(val);
    assertEquals(expected, actual);

    // make sure the ByteBuffer last byte filled in the buffer (its position) is same as before the
    // API call
    assertEquals(bbVisStartPos, bbVis.position());

    // vis: CharSequence (String implementation)
    actual = new Mutation("row5");
    actual.at().family(fam).qualifier(qual).visibility(new String(vis.getExpression())).put(val);
    assertEquals(expected, actual);

    // vis: ColumnVisibility
    actual = new Mutation("row5");
    actual.at().family(fam).qualifier(qual).visibility(vis).put(val);
    assertEquals(expected, actual);

    // vis: Text
    actual = new Mutation("row5");
    actual.at().family(fam).qualifier(qual).visibility(new Text(vis.getExpression())).put(val);
    assertEquals(expected, actual);
  }

  @Test
  public void testAtTimestampTypes() {
    final String fam = "f16bc";
    final String qual = "q1pm2";
    final long ts = 324324L;
    final String val = "v8672194923750";

    Mutation expected = new Mutation("row5");
    expected.put(fam, qual, ts, val);

    // Test timestamp method, keeping fam and val constant as Strings
    Mutation actual = new Mutation("row5");
    actual.at().family(fam).qualifier(qual).timestamp(ts).put(val);
    assertEquals(expected, actual);
  }

  @Test
  public void testAtPutTypes() {
    final String fam = "f16bc";
    final String qual = "q1pm2";
    final String val = "v8672194923750";

    Mutation expected = new Mutation("row5");
    expected.put(fam, qual, val);

    // Test all pull methods, keeping fam and qual,constant as Strings
    // put: byte[]
    Mutation actual = new Mutation("row5");
    actual.at().family(fam).qualifier(qual).put(val.getBytes(UTF_8));
    assertEquals(expected, actual);

    // put: ByteBuffer
    final ByteBuffer bbVal = ByteBuffer.wrap(val.getBytes(UTF_8));
    final int bbValStartPos = bbVal.position();
    actual = new Mutation("row5");
    actual.at().family(fam).qualifier(qual).put(bbVal);
    assertEquals(expected, actual);

    // make sure the ByteBuffer last byte filled in the buffer (its position) is same as before the
    // API call
    assertEquals(bbValStartPos, bbVal.position());

    // put: CharSequence (String implementation)
    actual = new Mutation("row5");
    actual.at().family(fam).qualifier(qual).put(val);
    assertEquals(expected, actual);

    // put: Text
    actual = new Mutation("row5");
    actual.at().family(fam).qualifier(qual).put(val);
    assertEquals(expected, actual);

    // put: Value
    actual = new Mutation("row5");
    actual.at().family(fam).qualifier(qual).put(new Value(val));
    assertEquals(expected, actual);
  }

  @Test
  public void testFluentPutNull() {
    final String fam = "f16bc";
    final String qual = "q1pm2";
    final String val = "v8672194923750";

    Mutation expected = new Mutation("row5");
    expected.put(fam, qual, val);

    Mutation actual = new Mutation("row5");
    actual.at().family(fam).qualifier(qual).put(val.getBytes());
    assertEquals(expected, actual);
    assertEquals(34, actual.numBytes());
    assertThrows(IllegalStateException.class,
        () -> actual.at().family(fam).qualifier(qual).put("test2"));
  }

  @Test
  public void testFluentPutLarge() {
    byte[] largeVal = new byte[Mutation.VALUE_SIZE_COPY_CUTOFF + 13];
    Arrays.fill(largeVal, (byte) 3);

    Mutation m = new Mutation("row123");
    m.at().family("fam").qualifier("qual").put(largeVal);
    assertEquals(32800, m.numBytes());
  }

  @Test
  public void testAtDelete() {
    final String fam = "f16bc";
    final String qual = "q1pm2";

    Mutation expected = new Mutation("row5");
    expected.putDelete(fam, qual);

    Mutation actual = new Mutation("row5");
    actual.at().family(fam).qualifier(qual).delete();
    assertEquals(expected, actual);
  }

  @Test
  public void testPuts() {
    Mutation m = new Mutation(new Text("r1"));

    m.put(nt("cf1"), nt("cq1"), nv("v1"));
    m.put(nt("cf2"), nt("cq2"), new ColumnVisibility("cv2"), nv("v2"));
    m.put(nt("cf3"), nt("cq3"), 3L, nv("v3"));
    m.put(nt("cf4"), nt("cq4"), new ColumnVisibility("cv4"), 4L, nv("v4"));

    m.putDelete(nt("cf5"), nt("cq5"));
    m.putDelete(nt("cf6"), nt("cq6"), new ColumnVisibility("cv6"));
    m.putDelete(nt("cf7"), nt("cq7"), 7L);
    m.putDelete(nt("cf8"), nt("cq8"), new ColumnVisibility("cv8"), 8L);

    assertEquals(8, m.size());

    List<ColumnUpdate> updates = m.getUpdates();

    assertEquals(8, m.size());
    assertEquals(8, updates.size());

    verifyColumnUpdate(updates.get(0), "cf1", "cq1", "", 0L, false, false, "v1");
    verifyColumnUpdate(updates.get(1), "cf2", "cq2", "cv2", 0L, false, false, "v2");
    verifyColumnUpdate(updates.get(2), "cf3", "cq3", "", 3L, true, false, "v3");
    verifyColumnUpdate(updates.get(3), "cf4", "cq4", "cv4", 4L, true, false, "v4");

    verifyColumnUpdate(updates.get(4), "cf5", "cq5", "", 0L, false, true, "");
    verifyColumnUpdate(updates.get(5), "cf6", "cq6", "cv6", 0L, false, true, "");
    verifyColumnUpdate(updates.get(6), "cf7", "cq7", "", 7L, true, true, "");
    verifyColumnUpdate(updates.get(7), "cf8", "cq8", "cv8", 8L, true, true, "");

  }

  @Test
  public void testPutsString() {
    Mutation m = new Mutation("r1");

    m.put("cf1", "cq1", nv("v1"));
    m.put("cf2", "cq2", new ColumnVisibility("cv2"), nv("v2"));
    m.put("cf3", "cq3", 3L, nv("v3"));
    m.put("cf4", "cq4", new ColumnVisibility("cv4"), 4L, nv("v4"));

    m.putDelete("cf5", "cq5");
    m.putDelete("cf6", "cq6", new ColumnVisibility("cv6"));
    m.putDelete("cf7", "cq7", 7L);
    m.putDelete("cf8", "cq8", new ColumnVisibility("cv8"), 8L);

    assertEquals(8, m.size());

    List<ColumnUpdate> updates = m.getUpdates();

    assertEquals(8, m.size());
    assertEquals(8, updates.size());

    verifyColumnUpdate(updates.get(0), "cf1", "cq1", "", 0L, false, false, "v1");
    verifyColumnUpdate(updates.get(1), "cf2", "cq2", "cv2", 0L, false, false, "v2");
    verifyColumnUpdate(updates.get(2), "cf3", "cq3", "", 3L, true, false, "v3");
    verifyColumnUpdate(updates.get(3), "cf4", "cq4", "cv4", 4L, true, false, "v4");

    verifyColumnUpdate(updates.get(4), "cf5", "cq5", "", 0L, false, true, "");
    verifyColumnUpdate(updates.get(5), "cf6", "cq6", "cv6", 0L, false, true, "");
    verifyColumnUpdate(updates.get(6), "cf7", "cq7", "", 7L, true, true, "");
    verifyColumnUpdate(updates.get(7), "cf8", "cq8", "cv8", 8L, true, true, "");
  }

  @Test
  public void testPutsStringString() {
    Mutation m = new Mutation("r1");

    m.put("cf1", "cq1", "v1");
    m.put("cf2", "cq2", new ColumnVisibility("cv2"), "v2");
    m.put("cf3", "cq3", 3L, "v3");
    m.put("cf4", "cq4", new ColumnVisibility("cv4"), 4L, "v4");

    m.putDelete("cf5", "cq5");
    m.putDelete("cf6", "cq6", new ColumnVisibility("cv6"));
    m.putDelete("cf7", "cq7", 7L);
    m.putDelete("cf8", "cq8", new ColumnVisibility("cv8"), 8L);

    assertEquals(8, m.size());
    assertEquals("r1", new String(m.getRow()));

    List<ColumnUpdate> updates = m.getUpdates();

    assertEquals(8, m.size());
    assertEquals(8, updates.size());

    verifyColumnUpdate(updates.get(0), "cf1", "cq1", "", 0L, false, false, "v1");
    verifyColumnUpdate(updates.get(1), "cf2", "cq2", "cv2", 0L, false, false, "v2");
    verifyColumnUpdate(updates.get(2), "cf3", "cq3", "", 3L, true, false, "v3");
    verifyColumnUpdate(updates.get(3), "cf4", "cq4", "cv4", 4L, true, false, "v4");

    verifyColumnUpdate(updates.get(4), "cf5", "cq5", "", 0L, false, true, "");
    verifyColumnUpdate(updates.get(5), "cf6", "cq6", "cv6", 0L, false, true, "");
    verifyColumnUpdate(updates.get(6), "cf7", "cq7", "", 7L, true, true, "");
    verifyColumnUpdate(updates.get(7), "cf8", "cq8", "cv8", 8L, true, true, "");
  }

  @Test
  public void testByteArrays() {
    Mutation m = new Mutation("r1".getBytes());

    m.put("cf1".getBytes(), "cq1".getBytes(), "v1".getBytes());
    m.put("cf2".getBytes(), "cq2".getBytes(), new ColumnVisibility("cv2"), "v2".getBytes());
    m.put("cf3".getBytes(), "cq3".getBytes(), 3L, "v3".getBytes());
    m.put("cf4".getBytes(), "cq4".getBytes(), new ColumnVisibility("cv4"), 4L, "v4".getBytes());

    m.putDelete("cf5".getBytes(), "cq5".getBytes());
    m.putDelete("cf6".getBytes(), "cq6".getBytes(), new ColumnVisibility("cv6"));
    m.putDelete("cf7".getBytes(), "cq7".getBytes(), 7L);
    m.putDelete("cf8".getBytes(), "cq8".getBytes(), new ColumnVisibility("cv8"), 8L);

    assertEquals(8, m.size());

    List<ColumnUpdate> updates = m.getUpdates();

    assertEquals(8, m.size());
    assertEquals(8, updates.size());

    verifyColumnUpdate(updates.get(0), "cf1", "cq1", "", 0L, false, false, "v1");
    verifyColumnUpdate(updates.get(1), "cf2", "cq2", "cv2", 0L, false, false, "v2");
    verifyColumnUpdate(updates.get(2), "cf3", "cq3", "", 3L, true, false, "v3");
    verifyColumnUpdate(updates.get(3), "cf4", "cq4", "cv4", 4L, true, false, "v4");

    verifyColumnUpdate(updates.get(4), "cf5", "cq5", "", 0L, false, true, "");
    verifyColumnUpdate(updates.get(5), "cf6", "cq6", "cv6", 0L, false, true, "");
    verifyColumnUpdate(updates.get(6), "cf7", "cq7", "", 7L, true, true, "");
    verifyColumnUpdate(updates.get(7), "cf8", "cq8", "cv8", 8L, true, true, "");
  }

  /**
   * Test for regression on bug 3422. If a {@link Mutation} object is reused for multiple calls to
   * readFields, the mutation would previously be "locked in" to the first set of column updates
   * (and value lengths). Hadoop input formats reuse objects when reading, so if Mutations are used
   * with an input format (or as the input to a combiner or reducer) then they will be used in this
   * fashion.
   */
  @Test
  public void testMultipleReadFieldsCalls() throws IOException {
    // Create test mutations and write them to a byte output stream
    Mutation m1 = new Mutation("row1");
    m1.put("cf1.1", "cq1.1", new ColumnVisibility("A|B"), "val1.1");
    m1.put("cf1.2", "cq1.2", new ColumnVisibility("C|D"), "val1.2");
    byte[] val1_3 = new byte[Mutation.VALUE_SIZE_COPY_CUTOFF + 3];
    Arrays.fill(val1_3, (byte) 3);
    m1.put("cf1.3", "cq1.3", new ColumnVisibility("E|F"), new String(val1_3));
    int size1 = m1.size();
    long nb1 = m1.numBytes();

    Mutation m2 = new Mutation("row2");
    byte[] val2 = new byte[Mutation.VALUE_SIZE_COPY_CUTOFF + 2];
    Arrays.fill(val2, (byte) 2);
    m2.put("cf2", "cq2", new ColumnVisibility("G|H"), 1234, new String(val2));
    int size2 = m2.size();
    long nb2 = m2.numBytes();

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    m1.write(dos);
    m2.write(dos);
    dos.close();

    // Now read the mutations back in from the byte array, making sure to
    // reuse the same mutation object, and make sure everything is correct.
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    DataInputStream dis = new DataInputStream(bis);

    Mutation m = new Mutation();
    m.readFields(dis);

    assertEquals("row1", new String(m.getRow()));
    assertEquals(size1, m.size());
    assertEquals(nb1, m.numBytes());
    assertEquals(3, m.getUpdates().size());
    verifyColumnUpdate(m.getUpdates().get(0), "cf1.1", "cq1.1", "A|B", 0L, false, false, "val1.1");
    verifyColumnUpdate(m.getUpdates().get(1), "cf1.2", "cq1.2", "C|D", 0L, false, false, "val1.2");
    verifyColumnUpdate(m.getUpdates().get(2), "cf1.3", "cq1.3", "E|F", 0L, false, false,
        new String(val1_3));

    // Reuse the same mutation object (which is what happens in the hadoop framework
    // when objects are read by an input format)
    m.readFields(dis);

    assertEquals("row2", new String(m.getRow()));
    assertEquals(size2, m.size());
    assertEquals(nb2, m.numBytes());
    assertEquals(1, m.getUpdates().size());
    verifyColumnUpdate(m.getUpdates().get(0), "cf2", "cq2", "G|H", 1234L, true, false,
        new String(val2));
  }

  private void verifyColumnUpdate(ColumnUpdate cu, String cf, String cq, String cv, long ts,
      boolean timeSet, boolean deleted, String val) {

    assertEquals(cf, new String(cu.getColumnFamily()));
    assertEquals(cq, new String(cu.getColumnQualifier()));
    assertEquals(cv, new String(cu.getColumnVisibility()));
    assertEquals(timeSet, cu.hasTimestamp());
    if (timeSet) {
      assertEquals(ts, cu.getTimestamp());
    }
    assertEquals(deleted, cu.isDeleted());
    assertEquals(val, new String(cu.getValue()));
  }

  @Test
  public void test4() throws Exception {
    Mutation m1 = new Mutation(new Text("r1"));

    m1.put(nt("cf1"), nt("cq1"), nv("v1"));
    m1.put(nt("cf2"), nt("cq2"), new ColumnVisibility("cv2"), nv("v2"));

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    m1.write(dos);
    dos.close();

    Mutation m2 = new Mutation(new Text("r2"));

    m2.put(nt("cf3"), nt("cq3"), nv("v3"));
    m2.put(nt("cf4"), nt("cq4"), new ColumnVisibility("cv2"), nv("v4"));

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    DataInputStream dis = new DataInputStream(bis);

    // used to be a bug where puts done before readFields would be seen
    // after readFields
    m2.readFields(dis);

    assertEquals("r1", new String(m2.getRow()));
    assertEquals(2, m2.getUpdates().size());
    assertEquals(2, m2.size());
    verifyColumnUpdate(m2.getUpdates().get(0), "cf1", "cq1", "", 0L, false, false, "v1");
    verifyColumnUpdate(m2.getUpdates().get(1), "cf2", "cq2", "cv2", 0L, false, false, "v2");
  }

  Mutation convert(OldMutation old) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    old.write(dos);
    dos.close();
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    DataInputStream dis = new DataInputStream(bis);
    Mutation m = new Mutation();
    m.readFields(dis);
    dis.close();
    return m;
  }

  @Test
  public void testNewSerialization() throws Exception {
    // write an old mutation
    OldMutation m2 = new OldMutation("r1");
    m2.put("cf1", "cq1", "v1");
    m2.put("cf2", "cq2", new ColumnVisibility("cv2"), "v2");
    m2.putDelete("cf3", "cq3");
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    m2.write(dos);
    dos.close();
    long oldSize = dos.size();
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    DataInputStream dis = new DataInputStream(bis);
    m2.readFields(dis);
    dis.close();

    // check it
    assertEquals("r1", new String(m2.getRow()));
    assertEquals(3, m2.getUpdates().size());
    assertEquals(3, m2.size());
    verifyColumnUpdate(m2.getUpdates().get(0), "cf1", "cq1", "", 0L, false, false, "v1");
    verifyColumnUpdate(m2.getUpdates().get(1), "cf2", "cq2", "cv2", 0L, false, false, "v2");
    verifyColumnUpdate(m2.getUpdates().get(2), "cf3", "cq3", "", 0L, false, true, "");

    Mutation m1 = convert(m2);

    assertEquals("r1", new String(m1.getRow()));
    assertEquals(3, m1.getUpdates().size());
    assertEquals(3, m1.size());
    verifyColumnUpdate(m1.getUpdates().get(0), "cf1", "cq1", "", 0L, false, false, "v1");
    verifyColumnUpdate(m1.getUpdates().get(1), "cf2", "cq2", "cv2", 0L, false, false, "v2");
    verifyColumnUpdate(m1.getUpdates().get(2), "cf3", "cq3", "", 0L, false, true, "");

    Text exampleRow = new Text(" 123456789 123456789 123456789 123456789 123456789");
    int exampleLen = exampleRow.getLength();
    m1 = new Mutation(exampleRow);
    m1.put("", "", "");

    bos = new ByteArrayOutputStream();
    dos = new DataOutputStream(bos);
    m1.write(dos);
    dos.close();
    long newSize = dos.size();
    assertTrue(newSize < oldSize);
    assertEquals(10, newSize - exampleLen);
    assertEquals(68, oldSize - exampleLen);
    // I am converting to integer to avoid comparing floats which are inaccurate
    assertEquals(14705, (int) (((newSize - exampleLen) * 100. / (oldSize - exampleLen)) * 1000));
    StringBuilder sb = new StringBuilder();
    byte[] ba = bos.toByteArray();
    for (int i = 0; i < bos.size(); i += 4) {
      for (int j = i; j < bos.size() && j < i + 4; j++) {
        sb.append(String.format("%02x", ba[j]));
      }
      sb.append(" ");
    }
    assertEquals("80322031 32333435 36373839 20313233 34353637"
        + " 38392031 32333435 36373839 20313233 34353637"
        + " 38392031 32333435 36373839 06000000 00000001 ", sb.toString());

  }

  @Test
  public void testReserialize() throws Exception {
    // test reading in a new mutation from an old mutation and reserializing the new mutation...
    // this was failing
    OldMutation om = new OldMutation("r1");
    om.put("cf1", "cq1", "v1");
    om.put("cf2", "cq2", new ColumnVisibility("cv2"), "v2");
    om.putDelete("cf3", "cq3");
    StringBuilder bigVal = new StringBuilder();
    for (int i = 0; i < 100000; i++) {
      bigVal.append('a');
    }
    om.put("cf2", "big", bigVal);

    Mutation m1 = convert(om);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    m1.write(dos);
    dos.close();

    Mutation m2 = new Mutation();

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    DataInputStream dis = new DataInputStream(bis);
    m2.readFields(dis);

    assertEquals("r1", new String(m1.getRow()));
    assertEquals(4, m2.getUpdates().size());
    assertEquals(4, m2.size());
    verifyColumnUpdate(m2.getUpdates().get(0), "cf1", "cq1", "", 0L, false, false, "v1");
    verifyColumnUpdate(m2.getUpdates().get(1), "cf2", "cq2", "cv2", 0L, false, false, "v2");
    verifyColumnUpdate(m2.getUpdates().get(2), "cf3", "cq3", "", 0L, false, true, "");
    verifyColumnUpdate(m2.getUpdates().get(3), "cf2", "big", "", 0L, false, false,
        bigVal.toString());
  }

  // populate for testInitialBufferSizesEquals method
  private static void populate(Mutation... muts) {
    for (Mutation m : muts) {
      m.put("cf1", "cq1", "v1");
      m.put("cf1", "cq1", new ColumnVisibility("A&B"), "v2");
      m.put("cf1", "cq1", 3, "v3");
      m.put("cf1", "cq1", new ColumnVisibility("A&B&C"), 4, "v4");
      m.putDelete("cf2", "cf3");
      m.putDelete("cf2", "cf4", 3);
      m.putDelete("cf2", "cf4", new ColumnVisibility("A&B&C"), 3);
    }
  }

  @Test
  public void testInitialBufferSizesEquals() {
    // m1 uses CharSequence constructor
    Mutation m1 = new Mutation("r1");
    // m2 uses a different buffer size
    Mutation m2 = new Mutation("r1", 4242);
    // m3 uses Text constructor
    Mutation m3 = new Mutation(new Text("r1"));
    // m4 uses a different buffer size
    Mutation m4 = new Mutation(new Text("r1"), 4242);
    // m5 uses bytes constructor with offset/length
    byte[] r1Bytes = "r1".getBytes(UTF_8);
    Mutation m5 = new Mutation(r1Bytes);
    // m6 uses a different buffer size
    Mutation m6 = new Mutation(r1Bytes, 4242);
    // m7 uses bytes constructor with offset/length
    Mutation m7 = new Mutation(r1Bytes, 0, r1Bytes.length);
    // m8 uses a different buffer size
    Mutation m8 = new Mutation(r1Bytes, 0, r1Bytes.length, 4242);

    Mutation[] muts = {m1, m2, m3, m4, m5, m6, m7, m8};
    populate(muts);

    for (Mutation m : muts) {
      assertEquals(m1, m);
    }
  }

  @Test
  public void testEquals() {
    Mutation m1 = new Mutation("r1");

    m1.put("cf1", "cq1", "v1");
    m1.put("cf1", "cq1", new ColumnVisibility("A&B"), "v2");
    m1.put("cf1", "cq1", 3, "v3");
    m1.put("cf1", "cq1", new ColumnVisibility("A&B&C"), 4, "v4");
    m1.putDelete("cf2", "cf3");
    m1.putDelete("cf2", "cf4", 3);
    m1.putDelete("cf2", "cf4", new ColumnVisibility("A&B&C"), 3);

    // m2 has same data as m1
    Mutation m2 = new Mutation("r1");

    m2.put("cf1", "cq1", "v1");
    m2.put("cf1", "cq1", new ColumnVisibility("A&B"), "v2");
    m2.put("cf1", "cq1", 3, "v3");
    m2.put("cf1", "cq1", new ColumnVisibility("A&B&C"), 4, "v4");
    m2.putDelete("cf2", "cf3");
    m2.putDelete("cf2", "cf4", 3);
    m2.putDelete("cf2", "cf4", new ColumnVisibility("A&B&C"), 3);

    // m3 has different row than m1
    Mutation m3 = new Mutation("r2");

    m3.put("cf1", "cq1", "v1");
    m3.put("cf1", "cq1", new ColumnVisibility("A&B"), "v2");
    m3.put("cf1", "cq1", 3, "v3");
    m3.put("cf1", "cq1", new ColumnVisibility("A&B&C"), 4, "v4");
    m3.putDelete("cf2", "cf3");
    m3.putDelete("cf2", "cf4", 3);
    m3.putDelete("cf2", "cf4", new ColumnVisibility("A&B&C"), 3);

    // m4 has a different column than m1
    Mutation m4 = new Mutation("r1");

    m4.put("cf2", "cq1", "v1");
    m4.put("cf1", "cq1", new ColumnVisibility("A&B"), "v2");
    m4.put("cf1", "cq1", 3, "v3");
    m4.put("cf1", "cq1", new ColumnVisibility("A&B&C"), 4, "v4");
    m4.putDelete("cf2", "cf3");
    m4.putDelete("cf2", "cf4", 3);
    m4.putDelete("cf2", "cf4", new ColumnVisibility("A&B&C"), 3);

    // m5 has a different value than m1
    Mutation m5 = new Mutation("r1");

    m5.put("cf1", "cq1", "v1");
    m5.put("cf1", "cq1", new ColumnVisibility("A&B"), "v2");
    m5.put("cf1", "cq1", 3, "v4");
    m5.put("cf1", "cq1", new ColumnVisibility("A&B&C"), 4, "v4");
    m5.putDelete("cf2", "cf3");
    m5.putDelete("cf2", "cf4", 3);
    m5.putDelete("cf2", "cf4", new ColumnVisibility("A&B&C"), 3);

    assertEquals(m1, m1);
    assertEquals(m1, m2);
    assertEquals(m2, m1);
    assertEquals(m2.hashCode(), m1.hashCode());
    assertNotEquals(0, m1.hashCode());
    assertFalse(m1.equals(m3));
    assertFalse(m3.equals(m1));
    assertFalse(m1.equals(m4));
    assertFalse(m4.equals(m1));
    assertFalse(m3.equals(m4));
    assertFalse(m1.equals(m5));
    assertFalse(m5.equals(m1));
    assertFalse(m3.equals(m5));
    assertFalse(m4.equals(m5));
  }

  @Test
  public void testThrift() {
    Mutation m1 = new Mutation("r1");
    m1.put("cf1", "cq1", "v1");
    TMutation tm1 = m1.toThrift();
    Mutation m2 = new Mutation(tm1);
    assertEquals(m1, m2);
  }

  @Test
  public void testThrift_Invalid() {
    Mutation m1 = new Mutation("r1");
    m1.put("cf1", "cq1", "v1");
    TMutation tm1 = m1.toThrift();
    tm1.setRow((byte[]) null);
    assertThrows(IllegalArgumentException.class, () -> new Mutation(tm1));
  }

  /*
   * The following two tests assert that no exception is thrown after calling hashCode or equals on
   * a Mutation. These guard against the condition noted in ACCUMULO-3718.
   */
  @Test
  public void testPutAfterHashCode() {
    Mutation m = new Mutation("r");
    m.hashCode();
    try {
      m.put("cf", "cq", "v");
    } catch (IllegalStateException e) {
      fail("Calling Mutation#hashCode then Mutation#put should not result in an"
          + " IllegalStateException.");
    }
  }

  @Test
  public void testPutAfterEquals() {
    Mutation m = new Mutation("r");
    Mutation m2 = new Mutation("r2");
    m.equals(m2);
    try {
      m.put("cf", "cq", "v");
      m2.put("cf", "cq", "v");
    } catch (IllegalStateException e) {
      fail("Calling Mutation#equals then Mutation#put should not result in an"
          + " IllegalStateException.");
    }
  }

  @Test
  public void testSanityCheck() {
    Mutation m = new Mutation("too big mutation");
    m.put("cf", "cq1", "v");
    m.estRowAndLargeValSize += (Long.MAX_VALUE / 2);
    assertThrows(IllegalArgumentException.class, () -> m.put("cf", "cq2", "v"));
  }

  @Test
  public void testPrettyPrint() {
    String row = "row";
    String fam1 = "fam1";
    String fam2 = "fam2";
    String qual1 = "qual1";
    String qual2 = "qual2";
    String value1 = "value1";

    Mutation m = new Mutation("row");
    m.put(fam1, qual1, value1);
    m.putDelete(fam2, qual2);
    m.getUpdates(); // serialize

    String expected = "mutation: " + row + "\n update: " + fam1 + ":" + qual1 + " value " + value1
        + "\n update: " + fam2 + ":" + qual2 + " value [delete]\n";

    assertEquals(expected, m.prettyPrint());
  }
}
