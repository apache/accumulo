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

package org.apache.accumulo.core.file;

import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

public class FileOperationsTest {

  public FileOperationsTest() {}

  /**
   * Test for filenames with +1 dot
   */
  @Test
  public void handlesFilenamesWithMoreThanOneDot() throws IOException {

    Boolean caughtException = false;
    FileSKVWriter writer = null;
    String filename = "target/test.file." + RFile.EXTENSION;
    File testFile = new File(filename);
    if (testFile.exists()) {
      FileUtils.forceDelete(testFile);
    }
    try {
      FileOperations fileOperations = FileOperations.getInstance();
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.getLocal(conf);
      AccumuloConfiguration acuconf = AccumuloConfiguration.getDefaultConfiguration();
      writer = fileOperations.openWriter(filename, fs, conf, acuconf);
      writer.close();
    } catch (Exception ex) {
      caughtException = true;
    } finally {
      if (writer != null) {
        writer.close();
      }
      FileUtils.forceDelete(testFile);
    }

    assertFalse("Should not throw with more than 1 dot in filename.", caughtException);
  }
}
