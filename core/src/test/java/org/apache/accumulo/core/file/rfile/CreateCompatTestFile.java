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
package org.apache.accumulo.core.file.rfile;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CreateCompatTestFile {

  public static Set<ByteSequence> newColFamSequence(String... colFams) {
    HashSet<ByteSequence> cfs = new HashSet<>();

    for (String cf : colFams) {
      cfs.add(new ArrayByteSequence(cf));
    }

    return cfs;
  }

  private static Key newKey(String row, String cf, String cq, String cv, long ts) {
    return new Key(row.getBytes(), cf.getBytes(), cq.getBytes(), cv.getBytes(), ts);
  }

  private static Value newValue(String val) {
    return new Value(val.getBytes());
  }

  private static String formatStr(String prefix, int i) {
    return String.format(prefix + "%06d", i);
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    CachableBlockFile.Writer _cbw = new CachableBlockFile.Writer(fs, new Path(args[0]), "gz", null, conf, DefaultConfiguration.getInstance());
    RFile.Writer writer = new RFile.Writer(_cbw, 1000);

    writer.startNewLocalityGroup("lg1", newColFamSequence(formatStr("cf_", 1), formatStr("cf_", 2)));

    for (int i = 0; i < 1000; i++) {
      writer.append(newKey(formatStr("r_", i), formatStr("cf_", 1), formatStr("cq_", 0), "", 1000 - i), newValue(i + ""));
      writer.append(newKey(formatStr("r_", i), formatStr("cf_", 2), formatStr("cq_", 0), "", 1000 - i), newValue(i + ""));
    }

    writer.startNewLocalityGroup("lg2", newColFamSequence(formatStr("cf_", 3)));

    for (int i = 0; i < 1000; i++) {
      writer.append(newKey(formatStr("r_", i), formatStr("cf_", 3), formatStr("cq_", 0), "", 1000 - i), newValue(i + ""));
    }

    writer.startDefaultLocalityGroup();

    for (int i = 0; i < 1000; i++) {
      writer.append(newKey(formatStr("r_", i), formatStr("cf_", 4), formatStr("cq_", 0), "", 1000 - i), newValue(i + ""));
    }

    writer.close();
    _cbw.close();
  }
}
