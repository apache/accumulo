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
package org.apache.accumulo.shell.commands;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

public class QuotedStringTokenizerTest {

  @Test
  public void testProperties() {
    String jsonProperty = "config -s tserver.compaction.major.service.meta.planner.opts.executors="
        + "[{\"name\":\"small\",\"type\":\"internal\",\"maxSize\":\"32M\",\"numThreads\":2},"
        + "{\"name\":\"huge\",\"type\":\"internal\",\"numThreads\":2}]";
    testTokens(jsonProperty,
        List.of("config", "-s", "tserver.compaction.major.service.meta.planner.opts.executors="
            + "[{\"name\":\"small\",\"type\":\"internal\",\"maxSize\":\"32M\",\"numThreads\":2},"
            + "{\"name\":\"huge\",\"type\":\"internal\",\"numThreads\":2}]"));

    jsonProperty = "config -s 'tserver.compaction.major.service.meta.planner.opts.executors="
        + "[{\"name\":\"small\",\"type\":\"internal\",\"maxSize\":\"32M\",\"numThreads\":2},"
        + "{\"name\":\"huge\",\"type\":\"internal\",\"numThreads\":2}]'";
    testTokens(jsonProperty,
        List.of("config", "-s", "tserver.compaction.major.service.meta.planner.opts.executors="
            + "[{\"name\":\"small\",\"type\":\"internal\",\"maxSize\":\"32M\",\"numThreads\":2},"
            + "{\"name\":\"huge\",\"type\":\"internal\",\"numThreads\":2}]"));

    testTokens("config -t accumulo.root -f table.custom.test",
        List.of("config", "-t", "accumulo.root", "-f", "table.custom.test"));

    String spaceWithForceProperty = "config -s table.custom.test.property=\"test\" -force";
    testTokens(spaceWithForceProperty,
        List.of("config", "-s", "table.custom.test.property=\"test\"", "-force"));
  }

  @Test
  public void testTableCreate() {
    String input = "createtable test -l locg1=fam1,fam2";
    testTokens(input, List.of("createtable", "test", "-l", "locg1=fam1,fam2"));
  }

  @Test
  public void testCharEscaping() {
    String input = "config -s general.custom.hexprop=\\x56 -force";
    testTokens(input, List.of("config", "-s", "general.custom.hexprop=V", "-force"));

    input = "config -s general.custom.spaceprop=hello\\ world -force";
    testTokens(input, List.of("config", "-s", "general.custom.spaceprop=hello world", "-force"));

    input = "config -s general.custom.singlequoteprop=things\\ like\\ \\'hello\\'\\ world -force";
    testTokens(input, List.of("config", "-s",
        "general.custom.singlequoteprop=things like 'hello' world", "-force"));

    input = "config -s general.custom.doublequoteprop=things\\ like\\ \"hello\"\\ world -force";
    testTokens(input, List.of("config", "-s",
        "general.custom.doublequoteprop=things like \"hello\" world", "-force"));
  }

  private void testTokens(String input, List<String> expectedTokens) {
    QuotedStringTokenizer tokenizer = new QuotedStringTokenizer(input);
    String[] tokens = tokenizer.getTokens();
    assertArrayEquals(expectedTokens.toArray(), tokens,
        "Generated tokens do not match expected values: " + Arrays.toString(tokens));
  }

}
