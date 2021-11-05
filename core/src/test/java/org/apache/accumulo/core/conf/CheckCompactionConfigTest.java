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
package org.apache.accumulo.core.conf;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.accumulo.core.spi.compaction.CheckCompactionConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckCompactionConfigTest {

  Logger log = LoggerFactory.getLogger(CheckCompactionConfigTest.class);

  @Rule
  public TestName testName = new TestName();

  @Rule
  public TemporaryFolder folder =
      new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  @Test
  public void testValidInput() throws IOException {
    //@formatter:off
    String inputString = "test.ci.common.accumulo.server.props=\\\n" +
            "tserver.compaction.major.service.cs1.planner=" +
            "org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner \\\n" +
            "tserver.compaction.major.service.cs1.planner.opts.executors=\\\n" +
            "[{'name':'small','type':'internal','maxSize':'16M','numThreads':8},\\\n" +
            "{'name':'medium','type':'internal','maxSize':'128M','numThreads':4},\\\n" +
            "{'name':'large','type':'internal','numThreads':2}]\n".replaceAll("'","\"");
    //@formatter:on
    String filePath = writeToFileAndReturnPath(inputString);

    log.info("Wrote to path: {}\nWith string:\n{}", filePath, inputString);

    CheckCompactionConfig.main(new String[] {filePath});
  }

  @Test
  public void testThrowsErrorForExternalWithNumThreads() throws IOException {
    //@formatter:off
    String inputString = "test.ci.common.accumulo.server.props=\\\n" +
            "tserver.compaction.major.service.cs1.planner=" +
            "org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner \\\n" +
            "tserver.compaction.major.service.cs1.planner.opts.executors=\\\n" +
            "[{'name':'small','type':'internal','maxSize':'16M','numThreads':8},\\\n" +
            "{'name':'medium','type':'internal','maxSize':'128M','numThreads':4},\\\n" +
            "{'name':'large','type':'external','numThreads':2}]\n".replaceAll("'","\"");
    //@formatter:on
    String expectedErrorMsg = "'numThreads' should not be specified for external compactions";

    String filePath = writeToFileAndReturnPath(inputString);
    log.info("Wrote to path: {}\nWith string:\n{}", filePath, inputString);

    var e = assertThrows(IllegalArgumentException.class,
        () -> CheckCompactionConfig.main(new String[] {filePath}));
    assertEquals(e.getMessage(), expectedErrorMsg);
  }

  private String writeToFileAndReturnPath(String fileInput) throws IOException {
    File file = folder.newFile(testName.getMethodName());
    try (FileWriter fileWriter = new FileWriter(file, UTF_8);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
      bufferedWriter.write(fileInput);
    }
    return file.getAbsolutePath();
  }
}
