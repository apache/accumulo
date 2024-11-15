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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.junit.jupiter.api.Test;

public class ListScansCommandTest {

  @Test
  public void testServerRegexPredicate() throws ParseException {
    testServerRegexPredicate(new ListScansCommand());
  }

  @Test
  public void testResourceGroupRegexPredicate() throws ParseException {
    testResourceGroupRegexPredicate(new ListScansCommand());
  }

  static void testServerRegexPredicate(Command cmd) throws ParseException {
    Options opts = cmd.getOptions();
    CommandLineParser parser = new DefaultParser();

    List<String> matching =
        List.of(".*:[0-9]*", "local.*:2000", "localhost:2000", "l.*:200[0-9].*");
    for (String serverRegex : matching) {
      var predicate = buildServerPredicate(opts, parser, serverRegex);
      assertTrue(predicate.test("localhost", 2000));
    }

    List<String> nonMatching = List.of(".*:[0-1]*", "local.*:2100", "localhost:3000", "localhost");
    for (String serverRegex : nonMatching) {
      var predicate = buildServerPredicate(opts, parser, serverRegex);
      assertFalse(predicate.test("localhost", 2000));
    }

  }

  static void testResourceGroupRegexPredicate(Command cmd) throws ParseException {
    Options opts = cmd.getOptions();
    CommandLineParser parser = new DefaultParser();

    List<String> matching = List.of(".*", "test.*", ".*group", "testgroup");
    for (String rgRegex : matching) {
      var predicate = buildResourceGroupPredicate(opts, parser, rgRegex);
      assertTrue(predicate.test("testgroup"));
    }

    List<String> nonMatching = List.of(".*gro", "test.*gr", "testgroup1", "tg.*");
    for (String rgRegex : nonMatching) {
      var predicate = buildResourceGroupPredicate(opts, parser, rgRegex);
      assertFalse(predicate.test("testgroup"));
    }
  }

  static BiPredicate<String,Integer> buildServerPredicate(Options opts, CommandLineParser parser,
      String serverRegex) throws ParseException {

    String[] args = {"-s", serverRegex};
    Option serverOpt = opts.getOption("-s");
    CommandLine cli = parser.parse(opts, args);

    return ListScansCommand.serverRegexPredicate(cli, serverOpt);
  }

  static Predicate<String> buildResourceGroupPredicate(Options opts, CommandLineParser parser,
      String rgRegex) throws ParseException {

    String[] args = {"-rg", rgRegex};
    Option serverOpt = opts.getOption("-rg");
    CommandLine cli = parser.parse(opts, args);

    return ListScansCommand.rgRegexPredicate(cli, serverOpt);
  }
}
