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

import static org.apache.accumulo.shell.commands.ListScansCommand.getServerOptValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import org.apache.accumulo.core.data.ResourceGroupId;
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
  public void testTetServerOptValue() throws ParseException {
    var cmd = new ListScansCommand();
    CommandLineParser parser = new DefaultParser();
    Options opts = cmd.getOptions();
    Option serverOpt = opts.getOption("-s");
    Option tserverOpt = opts.getOption("-ts");

    assertThrows(IllegalArgumentException.class,
        () -> getServerOptValue(
            parser.parse(opts, new String[] {"-s", "server:1", "-ts", "server:2"}), serverOpt,
            tserverOpt));
    assertEquals("server:1", getServerOptValue(parser.parse(opts, new String[] {"-s", "server:1"}),
        serverOpt, tserverOpt));
    assertEquals("server:2", getServerOptValue(parser.parse(opts, new String[] {"-ts", "server:2"}),
        serverOpt, tserverOpt));
  }

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
      for (String opt : List.of("-s", "-ts")) {
        var predicate = buildServerPredicate(opts, parser, opt, serverRegex);
        assertTrue(predicate.test("localhost", 2000));
      }
    }

    List<String> nonMatching = List.of(".*:[0-1]*", "local.*:2100", "localhost:3000", "localhost");
    for (String serverRegex : nonMatching) {
      for (String opt : List.of("-s", "-ts")) {
        var predicate = buildServerPredicate(opts, parser, opt, serverRegex);
        assertFalse(predicate.test("localhost", 2000));
      }
    }

  }

  static void testResourceGroupRegexPredicate(Command cmd) throws ParseException {
    Options opts = cmd.getOptions();
    CommandLineParser parser = new DefaultParser();

    List<String> matching = List.of(".*", "test.*", ".*group", "testgroup");
    for (String rgRegex : matching) {
      var predicate = buildResourceGroupPredicate(opts, parser, rgRegex);
      assertTrue(predicate.test(ResourceGroupId.of("testgroup")));
    }

    List<String> nonMatching = List.of(".*gro", "test.*gr", "testgroup1", "tg.*");
    for (String rgRegex : nonMatching) {
      var predicate = buildResourceGroupPredicate(opts, parser, rgRegex);
      assertFalse(predicate.test(ResourceGroupId.of("testgroup")));
    }
  }

  static BiPredicate<String,Integer> buildServerPredicate(Options opts, CommandLineParser parser,
      String opt, String serverRegex) throws ParseException {

    // Test flags for server regex
    String[] args = {opt, serverRegex};
    Option serverOpt = opts.getOption(opt);
    CommandLine cli = parser.parse(opts, args);
    return ListScansCommand.serverRegexPredicate(cli.getOptionValue(serverOpt));
  }

  static Predicate<ResourceGroupId> buildResourceGroupPredicate(Options opts,
      CommandLineParser parser, String rgRegex) throws ParseException {

    // Test flag works for resource group regex
    String[] args = {"-rg", rgRegex};
    Option serverOpt = opts.getOption("-rg");
    CommandLine cli = parser.parse(opts, args);
    return ListScansCommand.rgRegexPredicate(cli.getOptionValue(serverOpt));
  }
}
