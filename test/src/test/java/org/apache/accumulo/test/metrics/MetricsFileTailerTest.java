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
package org.apache.accumulo.test.metrics;

import static org.junit.Assert.assertTrue;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileSystems;
import java.nio.file.Files;

import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsFileTailerTest {

  private static final Logger log = LoggerFactory.getLogger(MetricsFileTailerTest.class);

  private static final String TEST_OUTFILE_NAME = "/tmp/testfile.txt";
  private static final String SUCCESS = "success";

  @AfterClass
  public static void cleanup() {
    try {
      Files.deleteIfExists(FileSystems.getDefault().getPath(TEST_OUTFILE_NAME));
    } catch (IOException ex) {
      log.trace("Failed to clean-up test file " + TEST_OUTFILE_NAME, ex);
    }
  }

  /**
   * Create a file tailer and then write some lines and validate the tailer returns the last line.
   */
  @Test
  public void fileUpdates() throws InterruptedException {

    boolean passed = Boolean.FALSE;
    try (MetricsFileTailer tailer = new MetricsFileTailer("foo", TEST_OUTFILE_NAME)) {
      tailer.startDaemonThread();

      long lastUpdate = tailer.getLastUpdate();

      writeToFile();

      int count = 0;
      while (count++ < 5) {
        if (lastUpdate == tailer.getLastUpdate()) {
          log.trace("no change");
        } else {
          lastUpdate = tailer.getLastUpdate();
          log.trace("{} - {}", tailer.getLastUpdate(), tailer.getLast());
          if (SUCCESS.compareTo(tailer.getLast()) == 0) {
            passed = Boolean.TRUE;
            break;
          }
        }
        Thread.sleep(5_000);
      }

    }
    assertTrue(passed);
  }

  /**
   * Simulate write record(s) to the file.
   */
  private void writeToFile() {
    try (FileWriter writer = new FileWriter(TEST_OUTFILE_NAME, true);
        PrintWriter printWriter = new PrintWriter(writer)) {
      printWriter.println("foo");
      // needs to be last line for test to pass
      printWriter.println(SUCCESS);
      printWriter.flush();
    } catch (IOException ex) {
      throw new IllegalStateException("failed to write data to test file", ex);
    }
  }
}
