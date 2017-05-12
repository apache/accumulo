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
package org.apache.accumulo.test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;

public class CreateRandomRFile {
  private static int num;
  private static String file;

  public static byte[] createValue(long rowid, int dataSize) {
    Random r = new Random(rowid);
    byte value[] = new byte[dataSize];

    r.nextBytes(value);

    // transform to printable chars
    for (int j = 0; j < value.length; j++) {
      value[j] = (byte) (((0xff & value[j]) % 92) + ' ');
    }

    return value;
  }

  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Usage CreateRandomRFile <filename> <size>");
      System.exit(-1);
    }
    file = args[0];
    num = Integer.parseInt(args[1]);
    long rands[] = new long[num];

    Random r = new Random();

    for (int i = 0; i < rands.length; i++) {
      rands[i] = (r.nextLong() & 0x7fffffffffffffffl) % 10000000000l;
    }

    Arrays.sort(rands);

    Configuration conf = CachedConfiguration.getInstance();
    FileSKVWriter mfw;
    try {
      FileSystem fs = FileSystem.get(conf);
      mfw = new RFileOperations().newWriterBuilder().forFile(file, fs, conf).withTableConfiguration(DefaultConfiguration.getInstance()).build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    for (int i = 0; i < rands.length; i++) {
      Text row = new Text(String.format("row_%010d", rands[i]));
      Key key = new Key(row);

      Value dv = new Value(createValue(rands[i], 40));

      try {
        mfw.append(key, dv);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    try {
      mfw.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
