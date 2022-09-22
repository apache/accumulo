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
package org.apache.accumulo.core.file;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.spi.crypto.NoCryptoServiceFactory;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;

public class FileOperationsTest {

  /**
   * Test for filenames with +1 dot
   */
  @Test
  public void handlesFilenamesWithMoreThanOneDot() throws IOException {

    boolean caughtException = false;
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
      AccumuloConfiguration acuconf = DefaultConfiguration.getInstance();
      writer =
          fileOperations.newWriterBuilder().forFile(filename, fs, conf, NoCryptoServiceFactory.NONE)
              .withTableConfiguration(acuconf).build();
      writer.close();
    } catch (Exception ex) {
      caughtException = true;
    } finally {
      if (writer != null) {
        writer.close();
      }
      FileUtils.forceDelete(testFile);
    }

    assertFalse(caughtException, "Should not throw with more than 1 dot in filename.");
  }
}
