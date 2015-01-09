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

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.validators.PositiveInteger;

public class ContinuousScanner {

  static class Opts extends ContinuousWalk.Opts {
    @Parameter(names = "--numToScan", description = "Number rows to scan between sleeps", required = true, validateWith = PositiveInteger.class)
    long numToScan = 0;
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    ScannerOpts scanOpts = new ScannerOpts();
    opts.parseArgs(ContinuousScanner.class.getName(), args, scanOpts);

    Random r = new Random();

    long distance = 1000000000000l;

    Connector conn = opts.getConnector();
    Authorizations auths = opts.randomAuths.getAuths(r);
    Scanner scanner = ContinuousUtil.createScanner(conn, opts.getTableName(), auths);
    scanner.setBatchSize(scanOpts.scanBatchSize);

    double delta = Math.min(.05, .05 / (opts.numToScan / 1000.0));

    while (true) {
      long startRow = ContinuousIngest.genLong(opts.min, opts.max - distance, r);
      byte[] scanStart = ContinuousIngest.genRow(startRow);
      byte[] scanStop = ContinuousIngest.genRow(startRow + distance);

      scanner.setRange(new Range(new Text(scanStart), new Text(scanStop)));

      int count = 0;
      Iterator<Entry<Key,Value>> iter = scanner.iterator();

      long t1 = System.currentTimeMillis();

      while (iter.hasNext()) {
        Entry<Key,Value> entry = iter.next();
        ContinuousWalk.validate(entry.getKey(), entry.getValue());
        count++;
      }

      long t2 = System.currentTimeMillis();

      // System.out.println("P1 " +count +" "+((1-delta) * numToScan)+" "+((1+delta) * numToScan)+" "+numToScan);

      if (count < (1 - delta) * opts.numToScan || count > (1 + delta) * opts.numToScan) {
        if (count == 0) {
          distance = distance * 10;
          if (distance < 0)
            distance = 1000000000000l;
        } else {
          double ratio = (double) opts.numToScan / count;
          // move ratio closer to 1 to make change slower
          ratio = ratio - (ratio - 1.0) * (2.0 / 3.0);
          distance = (long) (ratio * distance);
        }

        // System.out.println("P2 "+delta +" "+numToScan+" "+distance+"  "+((double)numToScan/count ));
      }

      System.out.printf("SCN %d %s %d %d%n", t1, new String(scanStart, UTF_8), (t2 - t1), count);

      if (opts.sleepTime > 0)
        UtilWaitThread.sleep(opts.sleepTime);
    }

  }
}
