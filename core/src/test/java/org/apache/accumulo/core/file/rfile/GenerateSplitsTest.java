/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.file.rfile;

import static org.apache.accumulo.core.file.rfile.GenerateSplits.main;
import static org.apache.accumulo.core.file.rfile.RFileTest.newColFamByteSequence;
import static org.apache.accumulo.core.file.rfile.RFileTest.newKey;
import static org.apache.accumulo.core.file.rfile.RFileTest.newValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths provided by test")
public class GenerateSplitsTest {
  private static final Logger log = LoggerFactory.getLogger(GenerateSplitsTest.class);

  @ClassRule
  public static final TemporaryFolder tempFolder =
      new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  private static final RFileTest.TestRFile trf = new RFileTest.TestRFile(null);
  private static String rfilePath;
  private static String splitsFilePath;

  /**
   * Creates a test file with 84 bytes of data and 2 Locality groups.
   */
  @BeforeClass
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

    File file = tempFolder.newFile("testGenerateSplits.rf");
    try (var fileOutputStream = new FileOutputStream(file)) {
      fileOutputStream.write(trf.baos.toByteArray());
    }
    rfilePath = "file:" + file.getAbsolutePath();
    log.info("Wrote to file {}", rfilePath);

    File splitsFile = tempFolder.newFile("testSplitsFile");
    splitsFilePath = splitsFile.getAbsolutePath();
  }

  @Test
  public void testNum() throws Exception {
    List<String> args = List.of(rfilePath, "--num", "2", "-sf", splitsFilePath);
    log.info("Invoking GenerateSplits with {}", args);
    GenerateSplits.main(args.toArray(new String[0]));
    verifySplitsFile("r3", "r6");

    // test more splits requested than indices
    args = List.of(rfilePath, "--num", "6", "-sf", splitsFilePath);
    log.info("Invoking GenerateSplits with {}", args);
    GenerateSplits.main(args.toArray(new String[0]));
    verifySplitsFile("r1", "r2", "r3", "r4", "r5", "r6");
  }

  @Test
  public void testSplitSize() throws Exception {
    List<String> args = List.of(rfilePath, "-ss", "21", "-sf", splitsFilePath);
    log.info("Invoking GenerateSplits with {}", args);
    GenerateSplits.main(args.toArray(new String[0]));
    verifySplitsFile("r2", "r4", "r6");
  }

  private void verifySplitsFile(String... splits) throws IOException {
    String splitsFile = Files.readString(Paths.get(splitsFilePath));
    assertEquals(splits.length, splitsFile.split("\n").length);
    for (String s : splits)
      assertTrue("Did not find " + s + " in " + splitsFilePath, splitsFile.contains(s));
  }

  @Test
  public void testErrors() throws Exception {
    List<String> args = List.of("missingFile.rf", "-n", "2");
    log.info("Invoking GenerateSplits with {}", args);
    assertThrows(FileNotFoundException.class, () -> main(args.toArray(new String[0])));

    List<String> args2 = List.of(rfilePath);
    log.info("Invoking GenerateSplits with {}", args2);
    var e = assertThrows(IllegalArgumentException.class, () -> main(args2.toArray(new String[0])));
    assertTrue(e.getMessage(), e.getMessage().contains("Required number of splits or"));

    List<String> args3 = List.of(rfilePath, "-n", "2", "-ss", "40");
    log.info("Invoking GenerateSplits with {}", args3);
    e = assertThrows(IllegalArgumentException.class, () -> main(args3.toArray(new String[0])));
    assertTrue(e.getMessage(), e.getMessage().contains("Requested number of splits and"));

    List<String> args4 = List.of(tempFolder.newFolder("dir1").getAbsolutePath(),
        tempFolder.newFolder("dir2").getAbsolutePath(), "-n", "2");
    log.info("Invoking GenerateSplits with {}", args4);
    e = assertThrows(IllegalArgumentException.class, () -> main(args4.toArray(new String[0])));
    assertTrue(e.getMessage(), e.getMessage().contains("Only one directory can be specified"));
  }
}
