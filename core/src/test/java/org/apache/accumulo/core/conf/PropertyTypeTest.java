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
package org.apache.accumulo.core.conf;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.WithTestNames;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.file.rfile.RFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.base.Joiner;

public class PropertyTypeTest extends WithTestNames {

  private PropertyType type;

  @BeforeEach
  public void getPropertyTypeForTest() {
    if (testName().startsWith("testType")) {
      String tn = testName().substring("testType".length());
      try {
        type = PropertyType.valueOf(tn);
      } catch (IllegalArgumentException e) {
        throw new AssertionError("Unexpected test method for non-existent "
            + PropertyType.class.getSimpleName() + "." + tn);
      }
    }
  }

  @Test
  public void testGetFormatDescription() {
    assertEquals(
        "An arbitrary string of characters whose format is unspecified and\n"
            + "interpreted based on the context of the property to which it applies.",
        PropertyType.STRING.getFormatDescription());
    for (PropertyType type : PropertyType.values()) {
      if (type == PropertyType.PREFIX) {
        assertNull(type.getFormatDescription());
        continue;
      }
      assertNotNull(type.getFormatDescription(),
          "format description for %s is null".formatted(type.name()));
      assertEquals(type.getFormatDescription(), type.getFormatDescription().strip(),
          "format description for %s contains extraneous whitespace".formatted(type.name()));
    }
  }

  @Test
  public void testToString() {
    assertEquals("string", PropertyType.STRING.toString());
  }

  /**
   * This test checks the remainder of the methods in this class to ensure each property type has a
   * corresponding test
   */
  @Test
  public void testFullCoverage() {

    String typePrefix = "testType";
    Set<String> typesTested = Stream.of(this.getClass().getMethods()).map(Method::getName)
        .filter(m -> m.startsWith(typePrefix)).map(m -> m.substring(typePrefix.length()))
        .collect(Collectors.toSet());

    Set<String> types =
        Stream.of(PropertyType.values()).map(Enum::name).collect(Collectors.toSet());

    assertEquals(types, typesTested, "Expected to see a test method for each property type");
  }

  private void valid(final String... args) {
    assertAll(() -> {
      for (String s : args) {
        assertTrue(type.isValidFormat(s),
            s + " should be valid for " + PropertyType.class.getSimpleName() + "." + type.name());
      }
    });
  }

  private void invalid(final String... args) {
    assertAll(() -> {
      for (String s : args) {
        assertFalse(type.isValidFormat(s),
            s + " should be invalid for " + PropertyType.class.getSimpleName() + "." + type.name());
      }
    });
  }

  @Test
  public void testTypeABSOLUTEPATH() {
    valid(null, "/foo", "/foo/c", "/", System.getProperty("user.dir"));
    // in Hadoop 2.x, Path only normalizes Windows paths properly when run on a Windows system
    // this makes the following checks fail
    if (System.getProperty("os.name").toLowerCase().contains("windows")) {
      valid("d:\\foo12", "c:\\foo\\g", "c:\\foo\\c", "c:\\");
    }
    invalid("foo12", "foo/g", "foo\\c");
  }

  @Test
  public void testTypeBOOLEAN() {
    valid(null, "True", "true", "False", "false", "tRUE", "fAlSe");
    invalid("foobar", "", "F", "T", "1", "0", "f", "t");
  }

  @Test
  public void testTypeCLASSNAME() {
    valid(null, "", String.class.getName(), String.class.getName() + "$1",
        String.class.getName() + "$TestClass");
    invalid("abc-def", "-", "!@#$%");
  }

  @Test
  public void testTypeCLASSNAMELIST() {
    testTypeCLASSNAME(); // test single class name
    valid(null, Joiner.on(",").join(String.class.getName(), String.class.getName() + "$1",
        String.class.getName() + "$TestClass"));
  }

  @Test
  public void testTypeCOUNT() {
    valid(null, "0", "1024", Long.toString(Integer.MAX_VALUE));
    invalid(Long.toString(Integer.MAX_VALUE + 1L), "-65535", "-1");
  }

  @Test
  public void testTypeDURABILITY() {
    valid(null, "none", "log", "flush", "sync");
    invalid("", "other");
  }

  @Test
  public void testTypeGC_POST_ACTION() {
    valid(null, "none", "flush", "compact");
    invalid("", "other");
  }

  @Test
  public void testTypeFRACTION() {
    valid(null, "1", "0", "1.0", "25%", "2.5%", "10.2E-3", "10.2E-3%", ".3");
    invalid("", "other", "20%%", "-0.3", "3.6a", "%25", "3%a");
  }

  @Test
  public void testTypeHOSTLIST() {
    valid(null, "localhost", "server1,server2,server3", "server1:1111,server2:3333",
        "localhost:1111", "server2:1111", "www.server", "www.server:1111", "www.server.com",
        "www.server.com:111");
    invalid(":111", "local host");
  }

  @Test
  public void testTypeBYTES() {
    valid(null, "1024", "20B", "100K", "1500M", "2G");
    invalid("1M500K", "1M 2K", "1MB", "1.5G", "1,024K", "", "a", "10%");
  }

  @Test
  public void testTypeMEMORY() {
    valid(null, "1024", "20B", "100K", "1500M", "2G", "10%");
    invalid("1M500K", "1M 2K", "1MB", "1.5G", "1,024K", "", "a");
  }

  @Test
  public void testTypePATH() {
    valid(null, "", "/absolute/path", "relative/path", "/with/trailing/slash/",
        "with/trailing/slash/");
  }

  @Test
  public void testTypePORT() {
    valid(null, "0", "1024", "30000", "65535");
    invalid("65536", "-65535", "-1", "1023");
  }

  @Test
  public void testTypeJSON() {
    valid("{\"y\":123}",
        "[{'name':'small','type':'internal','maxSize':'32M','numThreads':1},{'name':'huge','type':'internal','numThreads':1}]"
            .replaceAll("'", "\""));
    invalid("not json", "{\"x}", "{\"y\"", "{name:value}",
        "{ \"foo\" : \"bar\", \"foo\" : \"baz\" }", "{\"y\":123}extra");
  }

  @Test
  public void testTypePREFIX() {
    invalid(null, "", "whatever");
  }

  @Test
  public void testTypeSTRING() {
    valid(null, "", "whatever");
  }

  @Test
  public void testTypeTIMEDURATION() {
    valid(null, "600", "30s", "45m", "30000ms", "3d", "1h");
    invalid("1w", "1h30m", "1s 200ms", "ms", "", "a");
  }

  @Test
  public void testTypeURI() {
    valid(null, "", "hdfs://hostname", "file:///path/", "hdfs://example.com:port/path");
  }

  @Test
  public void testTypeFILENAME_EXT() {
    valid(RFile.EXTENSION, "rf");
    invalid(null, "RF", "map", "", "MAP", "rF", "Rf", " rf ");
  }

  @Test
  public void testTypeVOLUMES() {
    // more comprehensive parsing tests are in ConfigurationTypeHelperTest.testGetVolumeUris()
    valid("", "hdfs:/volA", ",hdfs:/volA", "hdfs:/volA,", "hdfs:/volA,file:/volB",
        ",hdfs:/volA,file:/volB", "hdfs:/volA,,file:/volB", "hdfs:/volA,file:/volB,   ,");
    invalid(null, "   ", ",", ",,,", " ,,,", ",,, ", ", ,,", "hdfs:/volA,hdfs:/volB,volA",
        ",volA,hdfs:/volA,hdfs:/volB", "hdfs:/volA,,volA,hdfs:/volB",
        "hdfs:/volA,volA,hdfs:/volB,   ,", "hdfs:/volA,hdfs:/volB,hdfs:/volA",
        "hdfs:/volA,hdfs :/::/volB");
  }

  @Test
  public void testTypeFATE_USER_CONFIG() {
    testFateConfig(Fate.FateOperation.getAllUserFateOps());
  }

  @Test
  public void testTypeFATE_META_CONFIG() {
    testFateConfig(Fate.FateOperation.getAllMetaFateOps());
  }

  private void testFateConfig(Set<Fate.FateOperation> allOps) {
    final int poolSize1 = allOps.size() / 2;
    final var validPool1Ops =
        allOps.stream().map(Enum::name).limit(poolSize1).collect(Collectors.joining(","));
    final var validPool2Ops =
        allOps.stream().map(Enum::name).skip(poolSize1).collect(Collectors.joining(","));
    final var allFateOpsStr = allOps.stream().map(Enum::name).collect(Collectors.joining(","));
    // should be valid: one pool for all ops, order should not matter, all ops split across
    // multiple pools (note validated in the same order as described here)
    valid(String.format("{'poolname':{'%s': 10}}", allFateOpsStr).replace("'", "\""),
        String.format("{'123abc':{'%s,%s': 10}}", validPool2Ops, validPool1Ops).replace("'", "\""),
        String.format("{'foo':{'%s': 2}, 'bar':{'%s': 3}}", validPool1Ops, validPool2Ops)
            .replace("'", "\""));

    var invalidPool1Ops =
        allOps.stream().map(Enum::name).limit(poolSize1).collect(Collectors.joining(","));
    var invalidPool2Ops =
        allOps.stream().map(Enum::name).skip(poolSize1 + 1).collect(Collectors.joining(","));
    // should be invalid: invalid json, null, missing FateOperation, pool size of 0, pool size of
    // -1, invalid pool size, invalid key, same FateOperation repeated in a different pool, invalid
    // FateOperation, long name, repeated name, more than one key/val for single pool name
    // (note validated in the same order as described here)
    invalid("", null,
        String.format("{'name1':{'%s': 2}, 'name2':{'%s': 3}}", invalidPool1Ops, invalidPool2Ops)
            .replace("'", "\""),
        String.format("{'foobar':{'%s': 0}}", allFateOpsStr).replace("'", "\""),
        String.format("{'foobar':{'%s': -1}}", allFateOpsStr).replace("'", "\""),
        String.format("{'foofoofoofoo':{'%s': x}}", allFateOpsStr).replace("'", "\""),
        String
            .format("{'123':{'%s': 10}}",
                allOps.stream().map(Enum::name).collect(Collectors.joining(", ")))
            .replace("'", "\""),
        String
            .format("{'abc':{'%s': 10}, 'def':{'%s': 10}}", allFateOpsStr,
                allOps.stream().map(Enum::name).limit(1).collect(Collectors.joining(",")))
            .replace("'", "\""),
        String.format("{'xyz':{'%s,INVALID_FATEOP': 10}}", allFateOpsStr).replace("'", "\""),
        String.format("{'%s':{'%s': 10}}", "x".repeat(100), allFateOpsStr).replace("'", "\""),
        String.format("{'name':{'%s':7}, 'name':{'%s':8}}", validPool1Ops, validPool2Ops)
            .replace("'", "\""),
        String.format("{'xyz123':{'%s':9,'%s':8}}", validPool1Ops, validPool2Ops).replace("'",
            "\""));
  }

  @Test
  public void testTypeFATE_THREADPOOL_SIZE() {
    // nothing to test, this type is used for a deprecated property and will accept any prop value.
  }

  @Test
  public void testTypeDROP_CACHE_SELECTION() {
    valid("all", "ALL", "NON-import", "NON-IMPORT", "non-import", "none", "NONE", "nOnE");
    invalid(null, "", "AL L", " ALL", "non import", "     ");
  }

  @Test
  public void testTypeCOMPRESSION_TYPE() {
    valid("none", "gz", "lz4", "snappy");
    // The following are valid at runtime with the correct configuration
    //
    // bzip2 java implementation does not implement Compressor/Decompressor, requires native
    // lzo not included in implementation due to license issues, but can be added by user
    // zstd requires hadoop native libraries built with zstd support
    //
    invalid(null, "", "bzip2", "lzo", "zstd");
  }

  @Test
  public void testTypeEC() {
    valid("enable", "ENABLE", "inherit", "INHERIT", "disable", "DISABLE");
    invalid(null, "policy", "XOR-2-1-1024k");
  }
}
