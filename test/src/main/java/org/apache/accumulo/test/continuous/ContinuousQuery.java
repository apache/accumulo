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

import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.test.continuous.ContinuousIngest.BaseOpts;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;

public class ContinuousQuery {

  public static class Opts extends BaseOpts {
    @Parameter(names = "--sleep", description = "the time to wait between queries", converter = TimeConverter.class)
    long sleepTime = 100;
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    ScannerOpts scanOpts = new ScannerOpts();
    opts.parseArgs(ContinuousQuery.class.getName(), args, scanOpts);

    Connector conn = opts.getConnector();
    Scanner scanner = ContinuousUtil.createScanner(conn, opts.getTableName(), opts.auths);
    scanner.setBatchSize(scanOpts.scanBatchSize);

    Random r = new Random();

    while (true) {
      byte[] row = ContinuousIngest.genRow(opts.min, opts.max, r);

      int count = 0;

      long t1 = System.currentTimeMillis();
      scanner.setRange(new Range(new Text(row)));
      for (Entry<Key,Value> entry : scanner) {
        ContinuousWalk.validate(entry.getKey(), entry.getValue());
        count++;
      }
      long t2 = System.currentTimeMillis();

      System.out.printf("SRQ %d %s %d %d%n", t1, new String(row, UTF_8), (t2 - t1), count);

      if (opts.sleepTime > 0)
        Thread.sleep(opts.sleepTime);
    }
  }
}
