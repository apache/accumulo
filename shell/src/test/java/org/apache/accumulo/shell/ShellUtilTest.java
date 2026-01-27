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
package org.apache.accumulo.shell;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class ShellUtilTest {

  @TempDir
  private static Path tempDir;

  // String with 3 lines, with one empty line
  private static final String FILEDATA = "line1\n\nline2";
  private static final String B64_FILEDATA =
      Base64.getEncoder().encodeToString("line1".getBytes(UTF_8)) + "\n\n"
          + Base64.getEncoder().encodeToString("line2".getBytes(UTF_8));

  @Test
  public void testWithoutDecode() throws IOException {
    Path testFile = tempDir.resolve("testFileNoDecode.txt");
    Files.writeString(testFile, FILEDATA, UTF_8);
    List<Text> output = ShellUtil.scanFile(testFile.toAbsolutePath().toString(), false);
    assertEquals(List.of(new Text("line1"), new Text("line2")), output);
  }

  @Test
  public void testWithDecode() throws IOException {
    Path testFile = tempDir.resolve("testFileWithDecode.txt");
    Files.writeString(testFile, B64_FILEDATA, UTF_8);
    List<Text> output = ShellUtil.scanFile(testFile.toAbsolutePath().toString(), true);
    assertEquals(List.of(new Text("line1"), new Text("line2")), output);
  }

  @Test
  public void testWithMissingFile() {
    assertThrows(NoSuchFileException.class, () -> ShellUtil.scanFile("missingFile.txt", false));
  }
}
