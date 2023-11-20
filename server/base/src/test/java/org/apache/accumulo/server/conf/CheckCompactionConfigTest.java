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
package org.apache.accumulo.server.conf;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.accumulo.server.WithTestNames;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path not set by user input")
public class CheckCompactionConfigTest extends WithTestNames {

  private final static Logger log = LoggerFactory.getLogger(CheckCompactionConfigTest.class);

  @TempDir
  private static File tempDir;

  @Test
  public void testValidInput1() throws Exception {
    String inputString = ("tserver.compaction.major.service.cs1.planner="
        + "org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner \n"
        + "tserver.compaction.major.service.cs1.planner.opts.executors=\\\n"
        + "[{'name':'small','type':'internal','maxSize':'16M','numThreads':8},\\\n"
        + "{'name':'medium','type':'internal','maxSize':'128M','numThreads':4},\\\n"
        + "{'name':'large','type':'internal','numThreads':2}]").replaceAll("'", "\"");

    String filePath = writeToFileAndReturnPath(inputString);

    CheckCompactionConfig.main(new String[] {filePath});
  }

  @Test
  public void testValidInput2() throws Exception {
    String inputString = ("tserver.compaction.major.service.cs1.planner="
        + "org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner \n"
        + "tserver.compaction.major.service.cs1.planner.opts.executors=\\\n"
        + "[{'name':'small','type':'internal','maxSize':'16M','numThreads':8},\\\n"
        + "{'name':'medium','type':'internal','maxSize':'128M','numThreads':4},\\\n"
        + "{'name':'large','type':'internal','numThreads':2}] \n"
        + "tserver.compaction.major.service.cs2.planner="
        + "org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner \n"
        + "tserver.compaction.major.service.cs2.planner.opts.executors=\\\n"
        + "[{'name':'small','type':'internal','maxSize':'16M','numThreads':7},\\\n"
        + "{'name':'medium','type':'internal','maxSize':'128M','numThreads':5},\\\n"
        + "{'name':'large','type':'external','queue':'DCQ1'}]").replaceAll("'", "\"");

    String filePath = writeToFileAndReturnPath(inputString);

    CheckCompactionConfig.main(new String[] {filePath});
  }

  @Test
  public void testValidInput3() throws Exception {
    String inputString = ("tserver.compaction.major.service.cs1.planner="
        + "org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner \n"
        + "tserver.compaction.major.service.cs1.planner.opts.executors=\\\n"
        + "[{'name':'small','type':'internal','maxSize':'16M','numThreads':8},\\\n"
        + "{'name':'medium','type':'internal','maxSize':'128M','numThreads':4},\\\n"
        + "{'name':'large','type':'internal','numThreads':2}] \n"
        + "tserver.compaction.major.service.cs2.planner="
        + "org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner \n"
        + "tserver.compaction.major.service.cs2.planner.opts.executors=\\\n"
        + "[{'name':'small','type':'internal','maxSize':'16M','numThreads':7},\\\n"
        + "{'name':'medium','type':'internal','maxSize':'128M','numThreads':5},\\\n"
        + "{'name':'large','type':'external','queue':'DCQ1'}] \n"
        + "compaction.service.cs3.planner="
        + "org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner \n"
        + "compaction.service.cs3.planner.opts.queues=\\\n"
        + "[{'name':'small','maxSize':'16M'},{'name':'large'}]").replaceAll("'", "\"");

    String filePath = writeToFileAndReturnPath(inputString);
    CheckCompactionConfig.main(new String[] {filePath});
  }

  @Test
  public void testThrowsExternalNumThreadsError() throws IOException {
    String inputString = ("tserver.compaction.major.service.cs1.planner="
        + "org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner \n"
        + "tserver.compaction.major.service.cs1.planner.opts.executors=\\\n"
        + "[{'name':'small','type':'internal','maxSize':'16M','numThreads':8},\\\n"
        + "{'name':'medium','type':'external','maxSize':'128M','numThreads':4},\\\n"
        + "{'name':'large','type':'internal','numThreads':2}]").replaceAll("'", "\"");
    String expectedErrorMsg = "'numThreads' should not be specified for external compactions";

    String filePath = writeToFileAndReturnPath(inputString);

    var e = assertThrows(IllegalArgumentException.class,
        () -> CheckCompactionConfig.main(new String[] {filePath}));
    assertEquals(expectedErrorMsg, e.getMessage());
  }

  @Test
  public void testNegativeThreadCount() throws IOException {
    String inputString = ("tserver.compaction.major.service.cs1.planner="
        + "org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner \n"
        + "tserver.compaction.major.service.cs1.planner.opts.executors=\\\n"
        + "[{'name':'small','type':'internal','maxSize':'16M','numThreads':8},\\\n"
        + "{'name':'medium','type':'internal','maxSize':'128M','numThreads':-4},\\\n"
        + "{'name':'large','type':'internal','numThreads':2}]").replaceAll("'", "\"");
    String expectedErrorMsg = "Positive number of threads required : -4";

    String filePath = writeToFileAndReturnPath(inputString);

    var e = assertThrows(IllegalArgumentException.class,
        () -> CheckCompactionConfig.main(new String[] {filePath}));
    assertEquals(e.getMessage(), expectedErrorMsg);
  }

  @Test
  public void testNoPlanner() throws Exception {
    String inputString = ("tserver.compaction.major.service.cs1.planner.opts.executors=\\\n"
        + "[{'name':'small','type':'internal','maxSize':'16M','numThreads':8},\\\n"
        + "{'name':'medium','type':'internal','maxSize':'128M','numThreads':4},\\\n"
        + "{'name':'large','type':'internal','numThreads':2}]").replaceAll("'", "\"");
    String expectedErrorMsg = "Incomplete compaction service definition, missing planner class";

    String filePath = writeToFileAndReturnPath(inputString);

    var e = assertThrows(IllegalArgumentException.class,
        () -> CheckCompactionConfig.main(new String[] {filePath}));
    assertTrue(e.getMessage().startsWith(expectedErrorMsg));
  }

  @Test
  public void testRepeatedCompactionExecutorID() throws Exception {
    String inputString = ("tserver.compaction.major.service.cs1.planner="
        + "org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner \n"
        + "tserver.compaction.major.service.cs1.planner.opts.executors=\\\n"
        + "[{'name':'small','type':'internal','maxSize':'16M','numThreads':8},\\\n"
        + "{'name':'medium','type':'internal','maxSize':'128M','numThreads':4},\\\n"
        + "{'name':'small','type':'internal','numThreads':2}]").replaceAll("'", "\"");
    String expectedErrorMsg = "Duplicate Compaction Executor ID found";

    final String filePath = writeToFileAndReturnPath(inputString);

    var e = assertThrows(IllegalStateException.class,
        () -> CheckCompactionConfig.main(new String[] {filePath}));
    assertTrue(e.getMessage().startsWith(expectedErrorMsg));
  }

  @Test
  public void testRepeatedQueueName() throws Exception {
    String inputString = ("compaction.service.cs1.planner="
        + "org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner \n"
        + "compaction.service.cs1.planner.opts.executors=\\\n"
        + "[{'name':'small','type':'external','maxSize':'16M','queue':'failedQueue'}] \n"
        + "compaction.service.cs1.planner.opts.queues=[{'name':'failedQueue'}]")
        .replaceAll("'", "\"");

    String expectedErrorMsg = "Duplicate external executor for queue failedQueue";

    final String filePath = writeToFileAndReturnPath(inputString);

    var err = assertThrows(IllegalArgumentException.class,
        () -> CheckCompactionConfig.main(new String[] {filePath}));
    assertEquals(err.getMessage(), expectedErrorMsg);
  }

  @Test
  public void testInvalidTypeValue() throws Exception {
    String inputString = ("tserver.compaction.major.service.cs1.planner="
        + "org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner \n"
        + "tserver.compaction.major.service.cs1.planner.opts.executors=\\\n"
        + "[{'name':'small','type':'internal','maxSize':'16M','numThreads':8},\\\n"
        + "{'name':'medium','type':'internal','maxSize':'128M','numThreads':4},\\\n"
        + "{'name':'large','type':'internl','numThreads':2}]").replaceAll("'", "\"");
    String expectedErrorMsg = "type must be 'internal' or 'external'";

    String filePath = writeToFileAndReturnPath(inputString);

    var e = assertThrows(IllegalArgumentException.class,
        () -> CheckCompactionConfig.main(new String[] {filePath}));
    assertTrue(e.getMessage().startsWith(expectedErrorMsg));
  }

  @Test
  public void testInvalidMaxSize() throws Exception {
    String inputString = ("tserver.compaction.major.service.cs1.planner="
        + "org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner \n"
        + "tserver.compaction.major.service.cs1.planner.opts.executors=\\\n"
        + "[{'name':'small','type':'internal','maxSize':'16M','numThreads':8},\\\n"
        + "{'name':'medium','type':'internal','maxSize':'0M','numThreads':4},\\\n"
        + "{'name':'large','type':'internal','numThreads':2}]").replaceAll("'", "\"");
    String expectedErrorMsg = "Invalid value for maxSize";

    String filePath = writeToFileAndReturnPath(inputString);

    var e = assertThrows(IllegalArgumentException.class,
        () -> CheckCompactionConfig.main(new String[] {filePath}));
    assertTrue(e.getMessage().startsWith(expectedErrorMsg));
  }

  @Test
  public void testBadPropsFilePath() {
    String[] args = {"/home/foo/bar/myProperties.properties"};
    String expectedErrorMsg = "File at given path could not be found";
    var e = assertThrows(FileNotFoundException.class, () -> CheckCompactionConfig.main(args));
    assertEquals(expectedErrorMsg, e.getMessage());
  }

  private String writeToFileAndReturnPath(String inputString) throws IOException {
    File file = new File(tempDir, testName() + ".properties");
    assertTrue(file.isFile() || file.createNewFile());
    try (FileWriter fileWriter = new FileWriter(file, UTF_8);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
      bufferedWriter.write(inputString);
    }
    log.info("Wrote to path: {}\nWith string:\n{}", file.getAbsolutePath(), inputString);
    return file.getAbsolutePath();
  }
}
