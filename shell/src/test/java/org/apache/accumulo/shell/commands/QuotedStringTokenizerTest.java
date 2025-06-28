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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class QuotedStringTokenizerTest {

  @Test
  public void testProperties() {
    String jsonProperty = "config -s tserver.compaction.major.service.meta.planner.opts.executors="
        + "[{\"name\":\"small\",\"type\":\"internal\",\"maxSize\":\"32M\",\"numThreads\":2},"
        + "{\"name\":\"huge\",\"type\":\"internal\",\"numThreads\":2}]";
    testTokens(jsonProperty, 3);

    testTokens("config -t accumulo.root -f table.custom.test", 5);

    String spaceWithForceProperty = "config -s table.custom.test.property=\"test\" -force";
    testTokens(spaceWithForceProperty, 4);
  }

  @Test
  public void testTableCreate() {
    String input = "createtable test -l locg1=fam1,fam2";
    testTokens(input, 4);
  }

  private void testTokens(String input, int expectedCount) {
    QuotedStringTokenizer tokenizer = new QuotedStringTokenizer(input);
    String[] tokens = tokenizer.getTokens();
    tokenizer.iterator().forEachRemaining(token -> {
      System.out.println("Token:" + token);
    });
    assertEquals(expectedCount, tokens.length,
        "Token count of " + tokens.length + " does not match expected value");
  }

}
