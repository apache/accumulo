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
package org.apache.accumulo.server.conf;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path not set by user input")
public class CheckCompactionConfigTest {

  private final static Logger log = LoggerFactory.getLogger(CheckCompactionConfigTest.class);

  @Rule
  public TestName testName = new TestName();

  @ClassRule
  public static final TemporaryFolder folder =
      new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  @Test
  public void testValidInput() throws Exception {
    //@formatter:off
    String inputString = ("test.ci.common.accumulo.server.props=\\\n" +
            "tserver.compaction.major.service.cs1.planner=" +
            "org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner \\\n" +
            "tserver.compaction.major.service.cs1.planner.opts.executors=\\\n" +
            "[{'name':'small','type':'internal','maxSize':'16M','numThreads':8},\\\n" +
            "{'name':'medium','type':'internal','maxSize':'128M','numThreads':4},\\\n" +
            "{'name':'large','type':'internal','numThreads':2}]").replaceAll("'","\"");
    //@formatter:on
    String filePath = writeToFileAndReturnPath(inputString);

    CheckCompactionConfig.main(new String[] {filePath});
  }

  @Test
  public void testBadPropsFilePath() {
    String[] args = {"/home/foo/bar/myProperties.properties"};
    String expectedErrorMsg = "File at given path could not be found";
    var e = assertThrows(FileNotFoundException.class, () -> CheckCompactionConfig.main(args));
    assertEquals(expectedErrorMsg, e.getMessage());
  }

  @Test
  public void testThrowsErrorForMultipleServerProps() throws IOException {
    //@formatter:off
    String inputString =
            "test.first.common.accumulo.server.props=\\\n" +
            "foo=bar\n\n" +
            "test.second.common.accumulo.server.props=\\\n" +
            "foo=bar";
    //@formatter:on
    String expectedErrorMsg = "expected one element but was:";

    String filePath = writeToFileAndReturnPath(inputString);

    var e = assertThrows(IllegalArgumentException.class,
        () -> CheckCompactionConfig.main(new String[] {filePath}));
    assertTrue(e.getMessage().contains(expectedErrorMsg));
  }

  private String writeToFileAndReturnPath(String inputString) throws IOException {
    File file = folder.newFile(testName.getMethodName() + ".properties");
    try (FileWriter fileWriter = new FileWriter(file, UTF_8);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
      bufferedWriter.write(inputString);
    }
    log.debug("Wrote to path: {}\nWith string:\n{}", file.getAbsolutePath(), inputString);
    return file.getAbsolutePath();
  }
}
