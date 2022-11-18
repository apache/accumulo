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
package org.apache.accumulo.shell;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

import org.apache.accumulo.core.util.Pair;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.hadoop.io.Text;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class ShellUtil {

  /**
   * Scans the given file line-by-line (ignoring empty lines) and returns a list containing those
   * lines. If decode is set to true, every line is decoded using {@link Base64} from the UTF-8
   * bytes of that line before inserting in the list.
   *
   * @param filename Path to the file that needs to be scanned
   * @param decode Whether to decode lines in the file
   * @return List of {@link Text} objects containing data in the given file
   */
  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "app is run in same security context as user providing the filename")
  public static List<Text> scanFile(String filename, boolean decode) throws IOException {
    String line;
    List<Text> result = new ArrayList<>();
    try (Scanner file = new Scanner(new File(filename), UTF_8)) {
      while (file.hasNextLine()) {
        line = file.nextLine();
        if (!line.isEmpty()) {
          result.add(decode ? new Text(Base64.getDecoder().decode(line)) : new Text(line));
        }
      }
    }
    return result;
  }

  public static Map<String,String> parseMapOpt(CommandLine cl, Option opt) {
    if (cl.hasOption(opt.getLongOpt())) {
      return Arrays.stream(cl.getOptionValue(opt.getLongOpt()).split(",")).map(kv -> {
        String[] sa = kv.split("=");
        return new Pair<>(sa[0], sa[1]);
      }).collect(Collectors.toUnmodifiableMap(Pair::getFirst, Pair::getSecond));
    } else {
      return Collections.emptyMap();
    }

  }
}
