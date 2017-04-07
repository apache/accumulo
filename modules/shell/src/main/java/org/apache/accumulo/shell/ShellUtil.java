/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.shell;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.io.Text;

public class ShellUtil {

  /**
   * Scans the given file line-by-line (ignoring empty lines) and returns a list containing those lines. If decode is set to true, every line is decoded using
   * {@link Base64} from the UTF-8 bytes of that line before inserting in the list.
   *
   * @param filename
   *          Path to the file that needs to be scanned
   * @param decode
   *          Whether to decode lines in the file
   * @return List of {@link Text} objects containing data in the given file
   * @throws FileNotFoundException
   *           if the given file doesn't exist
   */
  public static List<Text> scanFile(String filename, boolean decode) throws FileNotFoundException {
    String line;
    Scanner file = new Scanner(new File(filename), UTF_8.name());
    List<Text> result = new ArrayList<>();
    try {
      while (file.hasNextLine()) {
        line = file.nextLine();
        if (!line.isEmpty()) {
          result.add(decode ? new Text(Base64.getDecoder().decode(line)) : new Text(line));
        }
      }
    } finally {
      file.close();
    }
    return result;
  }
}
