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
package org.apache.accumulo.core.clientImpl.bulk;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Arrays;

import org.apache.accumulo.core.file.FileOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BulkImportFilterInvalidTest {
  FileSystem fs;
  Path testdir = new Path("testing");

  @BeforeEach
  public void setup() throws IOException {
    fs = FileSystem.getLocal(new Configuration());
    fs.mkdirs(testdir);
  }

  @AfterEach
  public void cleanup() throws IOException {
    fs.delete(testdir, true);
  }

  @Test
  public void testFilterInvalidGood() throws IOException {
    FileStatus[] files = new FileStatus[FileOperations.getValidExtensions().size()];
    int i = 0;
    for (String extension : FileOperations.getValidExtensions()) {
      String filename = "testFile." + extension;
      fs.createNewFile(new Path(testdir, filename));
      files[i++] = fs.getFileStatus(new Path(testdir, filename));
    }
    // all files should be valid
    assertEquals(i, BulkImport.filterInvalid(files).size());
    assertArrayEquals(files, BulkImport.filterInvalid(files).toArray());
  }

  @Test
  public void testFilterInvalidFile() throws IOException {
    FileStatus[] files = new FileStatus[2];
    int i = 0;
    // create file with no extension and an invalid extension
    for (String flag : Arrays.asList("testFile", "testFile.bad")) {
      fs.createNewFile(new Path(testdir, flag));
      files[i++] = fs.getFileStatus(new Path(testdir, flag));
    }
    assertEquals(0, BulkImport.filterInvalid(files).size());
  }

  @Test
  public void testFilterInvalidwithDir() throws IOException {
    String dir = "justadir";
    fs.mkdirs(new Path(testdir, dir));
    FileStatus[] files = new FileStatus[1];
    files[0] = fs.getFileStatus(new Path(testdir, dir));
    // no files should be valid
    assertEquals(0, BulkImport.filterInvalid(files).size());

  }

  @Test
  public void testFilterInvalidwithWorkingFile() throws IOException {
    FileStatus[] files = new FileStatus[FileOperations.getBulkWorkingFiles().size()];
    int i = 0;
    for (String workingfile : FileOperations.getBulkWorkingFiles()) {
      fs.createNewFile(new Path(testdir, workingfile));
      files[i++] = fs.getFileStatus(new Path(testdir, workingfile));
    }
    // no files should be valid
    assertEquals(0, BulkImport.filterInvalid(files).size());
  }

  @Test
  public void testFilterInvalidMixGoodBad() throws IOException {
    FileStatus[] files = new FileStatus[FileOperations.getValidExtensions().size() + 1];
    int i = 0;
    for (String extension : FileOperations.getValidExtensions()) {
      String filename = "testFile." + extension;
      fs.createNewFile(new Path(testdir, filename));
      files[i++] = fs.getFileStatus(new Path(testdir, filename));
    }
    // adding one more bad file so size is i+1
    fs.createNewFile(new Path(testdir, "testFile.bad"));
    files[i] = fs.getFileStatus(new Path(testdir, "testFile.bad"));
    // all files should be valid except the last
    assertEquals(i, BulkImport.filterInvalid(files).size());
    assertArrayEquals(Arrays.copyOf(files, i), BulkImport.filterInvalid(files).toArray());
  }
}
