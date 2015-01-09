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
package org.apache.accumulo.test.continuous;

import static com.google.common.base.Charsets.UTF_8;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.zip.CRC32;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.trace.instrument.Span;
import org.apache.accumulo.trace.instrument.Trace;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

public class ContinuousWalk {

  static public class Opts extends ContinuousQuery.Opts {
    class RandomAuthsConverter implements IStringConverter<RandomAuths> {
      @Override
      public RandomAuths convert(String value) {
        try {
          return new RandomAuths(value);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Parameter(names = "--authsFile", description = "read the authorities to use from a file")
    RandomAuths randomAuths = new RandomAuths();
  }

  static class BadChecksumException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public BadChecksumException(String msg) {
      super(msg);
    }

  }

  static class RandomAuths {
    private List<Authorizations> auths;

    RandomAuths() {
      auths = Collections.singletonList(Authorizations.EMPTY);
    }

    RandomAuths(String file) throws IOException {
      if (file == null) {
        auths = Collections.singletonList(Authorizations.EMPTY);
        return;
      }

      auths = new ArrayList<Authorizations>();

      FileSystem fs = FileSystem.get(new Configuration());
      BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(file)), UTF_8));
      try {
        String line;
        while ((line = in.readLine()) != null) {
          auths.add(new Authorizations(line.split(",")));
        }
      } finally {
        in.close();
      }
    }

    Authorizations getAuths(Random r) {
      return auths.get(r.nextInt(auths.size()));
    }
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(ContinuousWalk.class.getName(), args);

    Connector conn = opts.getConnector();

    Random r = new Random();

    ArrayList<Value> values = new ArrayList<Value>();

    while (true) {
      Scanner scanner = ContinuousUtil.createScanner(conn, opts.getTableName(), opts.randomAuths.getAuths(r));
      String row = findAStartRow(opts.min, opts.max, scanner, r);

      while (row != null) {

        values.clear();

        long t1 = System.currentTimeMillis();
        Span span = Trace.on("walk");
        try {
          scanner.setRange(new Range(new Text(row)));
          for (Entry<Key,Value> entry : scanner) {
            validate(entry.getKey(), entry.getValue());
            values.add(entry.getValue());
          }
        } finally {
          span.stop();
        }
        long t2 = System.currentTimeMillis();

        System.out.printf("SRQ %d %s %d %d%n", t1, row, (t2 - t1), values.size());

        if (values.size() > 0) {
          row = getPrevRow(values.get(r.nextInt(values.size())));
        } else {
          System.out.printf("MIS %d %s%n", t1, row);
          System.err.printf("MIS %d %s%n", t1, row);
          row = null;
        }

        if (opts.sleepTime > 0)
          Thread.sleep(opts.sleepTime);
      }

      if (opts.sleepTime > 0)
        Thread.sleep(opts.sleepTime);
    }
  }

  private static String findAStartRow(long min, long max, Scanner scanner, Random r) {

    byte[] scanStart = ContinuousIngest.genRow(min, max, r);
    scanner.setRange(new Range(new Text(scanStart), null));
    scanner.setBatchSize(100);

    int count = 0;
    String pr = null;

    long t1 = System.currentTimeMillis();

    for (Entry<Key,Value> entry : scanner) {
      validate(entry.getKey(), entry.getValue());
      pr = getPrevRow(entry.getValue());
      count++;
      if (pr != null)
        break;
    }

    long t2 = System.currentTimeMillis();

    System.out.printf("FSR %d %s %d %d%n", t1, new String(scanStart, UTF_8), (t2 - t1), count);

    return pr;
  }

  static int getPrevRowOffset(byte val[]) {
    if (val.length == 0)
      throw new IllegalArgumentException();
    if (val[53] != ':')
      throw new IllegalArgumentException(new String(val, UTF_8));

    // prev row starts at 54
    if (val[54] != ':') {
      if (val[54 + 16] != ':')
        throw new IllegalArgumentException(new String(val, UTF_8));
      return 54;
    }

    return -1;
  }

  static String getPrevRow(Value value) {

    byte[] val = value.get();
    int offset = getPrevRowOffset(val);
    if (offset > 0) {
      return new String(val, offset, 16, UTF_8);
    }

    return null;
  }

  static int getChecksumOffset(byte val[]) {
    if (val[val.length - 1] != ':') {
      if (val[val.length - 9] != ':')
        throw new IllegalArgumentException(new String(val, UTF_8));
      return val.length - 8;
    }

    return -1;
  }

  static void validate(Key key, Value value) throws BadChecksumException {
    int ckOff = getChecksumOffset(value.get());
    if (ckOff < 0)
      return;

    long storedCksum = Long.parseLong(new String(value.get(), ckOff, 8, UTF_8), 16);

    CRC32 cksum = new CRC32();

    cksum.update(key.getRowData().toArray());
    cksum.update(key.getColumnFamilyData().toArray());
    cksum.update(key.getColumnQualifierData().toArray());
    cksum.update(key.getColumnVisibilityData().toArray());
    cksum.update(value.get(), 0, ckOff);

    if (cksum.getValue() != storedCksum) {
      throw new BadChecksumException("Checksum invalid " + key + " " + value);
    }
  }
}
