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
package org.apache.accumulo.core.file.rfile;

import static org.apache.accumulo.core.file.rfile.GenerateSplits.getEvenlySpacedSplits;
import static org.apache.accumulo.core.file.rfile.GenerateSplits.main;
import static org.apache.accumulo.core.file.rfile.RFileTest.newColFamByteSequence;
import static org.apache.accumulo.core.file.rfile.RFileTest.newKey;
import static org.apache.accumulo.core.file.rfile.RFileTest.newValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths provided by test")
public class GenerateSplitsTest {
  private static final Logger log = LoggerFactory.getLogger(GenerateSplitsTest.class);

  @TempDir
  private static File tempDir;

  private static final RFileTest.TestRFile trf = new RFileTest.TestRFile(null);
  private static String rfilePath;
  private static String splitsFilePath;

  /**
   * Creates a test file with 84 bytes of data and 2 Locality groups.
   */
  @BeforeAll
  public static void createFile() throws IOException {
    trf.openWriter(false);
    trf.writer.startNewLocalityGroup("lg1", newColFamByteSequence("cf1", "cf2"));
    trf.writer.append(newKey("r1", "cf1", "cq1", "L1", 55), newValue("foo1"));
    trf.writer.append(newKey("r2", "cf2", "cq1", "L1", 55), newValue("foo2"));
    trf.writer.append(newKey("r3", "cf2", "cq1", "L1", 55), newValue("foo3"));
    trf.writer.startNewLocalityGroup("lg2", newColFamByteSequence("cf3", "cf4"));
    trf.writer.append(newKey("r4", "cf3", "cq1", "L1", 55), newValue("foo4"));
    trf.writer.append(newKey("r5", "cf4", "cq1", "L1", 55), newValue("foo5"));
    trf.writer.append(newKey("r6", "cf4", "cq1", "L1", 55), newValue("foo6"));
    trf.closeWriter();

    File file = new File(tempDir, "testGenerateSplits.rf");
    assertTrue(file.createNewFile(), "Failed to create file: " + file);
    try (var fileOutputStream = new FileOutputStream(file)) {
      fileOutputStream.write(trf.baos.toByteArray());
    }
    rfilePath = "file:" + file.getAbsolutePath();
    log.info("Wrote to file {}", rfilePath);

    File splitsFile = new File(tempDir, "testSplitsFile");
    assertTrue(splitsFile.createNewFile(), "Failed to create file: " + splitsFile);
    splitsFilePath = splitsFile.getAbsolutePath();
  }

  @Test
  public void testNum() throws Exception {
    List<String> args = List.of(rfilePath, "--num", "2", "-sf", splitsFilePath);
    log.info("Invoking GenerateSplits with {}", args);
    GenerateSplits.main(args.toArray(new String[0]));
    verifySplitsFile(false, "r6", "r3");

    // test more splits requested than indices
    args = List.of(rfilePath, "--num", "4", "-sf", splitsFilePath);
    log.info("Invoking GenerateSplits with {}", args);
    GenerateSplits.main(args.toArray(new String[0]));
    verifySplitsFile(false, "r2", "r3", "r4", "r5");
  }

  @Test
  public void testSplitSize() throws Exception {
    List<String> args = List.of(rfilePath, "-ss", "21", "-sf", splitsFilePath);
    log.info("Invoking GenerateSplits with {}", args);
    GenerateSplits.main(args.toArray(new String[0]));
    verifySplitsFile(false, "r2", "r4", "r6");
  }

  private void verifySplitsFile(boolean encoded, String... splits) throws IOException {
    String[] gSplits = Files.readString(Paths.get(splitsFilePath)).split("\n");
    assertEquals(splits.length, gSplits.length);
    TreeSet<String> expectedSplits =
        Arrays.stream(splits).collect(Collectors.toCollection(TreeSet::new));
    TreeSet<String> generatedSplits = Arrays.stream(gSplits).map(s -> decode(encoded, s))
        .map(String::new).collect(Collectors.toCollection(TreeSet::new));
    assertEquals(expectedSplits, generatedSplits,
        "Failed to find expected splits in " + generatedSplits);
  }

  private String decode(boolean decode, String string) {
    if (decode) {
      return new String(Base64.getDecoder().decode(string));
    }
    return string;
  }

  @Test
  public void testErrors() {
    List<String> args = List.of("missingFile.rf", "-n", "2");
    log.info("Invoking GenerateSplits with {}", args);
    assertThrows(FileNotFoundException.class, () -> main(args.toArray(new String[0])));

    List<String> args2 = List.of(rfilePath);
    log.info("Invoking GenerateSplits with {}", args2);
    var e = assertThrows(IllegalArgumentException.class, () -> main(args2.toArray(new String[0])));
    assertTrue(e.getMessage().contains("Required number of splits or"), e.getMessage());

    List<String> args3 = List.of(rfilePath, "-n", "2", "-ss", "40");
    log.info("Invoking GenerateSplits with {}", args3);
    e = assertThrows(IllegalArgumentException.class, () -> main(args3.toArray(new String[0])));
    assertTrue(e.getMessage().contains("Requested number of splits and"), e.getMessage());

    File dir1 = new File(tempDir, "dir1/");
    File dir2 = new File(tempDir, "dir2/");
    assertTrue(dir1.mkdir() && dir2.mkdir(), "Failed to make new sub-directories");

    List<String> args4 = List.of(dir1.getAbsolutePath(), dir2.getAbsolutePath(), "-n", "2");
    log.info("Invoking GenerateSplits with {}", args4);
    e = assertThrows(IllegalArgumentException.class, () -> main(args4.toArray(new String[0])));
    assertTrue(e.getMessage().contains("No files were found"), e.getMessage());
  }

  @Test
  public void testEvenlySpaced() {
    TreeSet<String> desired = getEvenlySpacedSplits(15, 4, numSplits(15));
    assertEquals(4, desired.size());
    assertEquals(Set.of("003", "006", "009", "012"), desired);
    desired = getEvenlySpacedSplits(15, 10, numSplits(15));
    assertEquals(10, desired.size());
    assertEquals(Set.of("001", "002", "004", "005", "006", "008", "009", "010", "012", "013"),
        desired);
    desired = getEvenlySpacedSplits(10, 9, numSplits(10));
    assertEquals(9, desired.size());
    assertEquals(Set.of("001", "002", "003", "004", "005", "006", "007", "008", "009"), desired);
  }

  @Test
  public void testNullValues() throws Exception {
    trf.openWriter(false);
    trf.writer.startNewLocalityGroup("lg1", newColFamByteSequence("cf1", "cf2"));
    trf.writer.append(newKey("r1\0a", "cf1", "cq1", "L1", 55), newValue("foo1"));
    trf.writer.append(newKey("r2\0b", "cf2", "cq1", "L1", 55), newValue("foo2"));
    trf.writer.append(newKey("r3\0c", "cf2", "cq1", "L1", 55), newValue("foo3"));
    trf.writer.startNewLocalityGroup("lg2", newColFamByteSequence("cf3", "cf4"));
    trf.writer.append(newKey("r4\0d", "cf3", "cq1", "L1", 55), newValue("foo4"));
    trf.writer.append(newKey("r5\0e", "cf4", "cq1", "L1", 55), newValue("foo5"));
    trf.writer.append(newKey("r6\0f", "cf4", "cq1", "L1", 55), newValue("foo6"));
    trf.closeWriter();

    File file = new File(tempDir, "testGenerateSplitsWithNulls.rf");
    assertTrue(file.createNewFile(), "Failed to create file: " + file);
    try (var fileOutputStream = new FileOutputStream(file)) {
      fileOutputStream.write(trf.baos.toByteArray());
    }
    rfilePath = "file:" + file.getAbsolutePath();
    log.info("Wrote to file {}", rfilePath);

    File splitsFile = new File(tempDir, "testSplitsFileWithNulls");
    assertTrue(splitsFile.createNewFile(), "Failed to create file: " + splitsFile);
    splitsFilePath = splitsFile.getAbsolutePath();

    List<String> finalArgs = List.of(rfilePath, "--num", "2", "-sf", splitsFilePath);
    assertThrows(UnsupportedOperationException.class,
        () -> GenerateSplits.main(finalArgs.toArray(new String[0])));

    List<String> args = List.of(rfilePath, "--num", "2", "-sf", splitsFilePath, "-b64");
    GenerateSplits.main(args.toArray(new String[0]));
    // Validate that base64 split points are working
    verifySplitsFile(true, "r6\0f", "r3\0c");
  }

  /**
   * Create the requested number of splits. Works up to 3 digits or max 999.
   */
  private Iterator<String> numSplits(int num) {
    TreeSet<String> splits = new TreeSet<>();
    for (int i = 0; i < num; i++) {
      splits.add(String.format("%03d", i));
    }
    return splits.iterator();
  }
}
