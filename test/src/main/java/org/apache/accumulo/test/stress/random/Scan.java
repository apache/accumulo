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
package org.apache.accumulo.test.stress.random;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Lists;

public class Scan {

  public static void main(String[] args) throws Exception {
    ScanOpts opts = new ScanOpts();
    opts.parseArgs(Scan.class.getName(), args);

    Connector connector = opts.getConnector();
    Scanner scanner = connector.createScanner(opts.getTableName(), new Authorizations());

    if (opts.isolate) {
      scanner.enableIsolation();
    }

    Random tablet_index_generator = new Random(opts.scan_seed);

    LoopControl scanning_condition = opts.continuous ? new ContinuousLoopControl() : new IterativeLoopControl(opts.scan_iterations);

    while (scanning_condition.keepScanning()) {
      Range range = pickRange(connector.tableOperations(), opts.getTableName(), tablet_index_generator);
      scanner.setRange(range);
      if (opts.batch_size > 0) {
        scanner.setBatchSize(opts.batch_size);
      }
      try {
        consume(scanner);
      } catch (Exception e) {
        System.err.println(String.format("Exception while scanning range %s. Check the state of Accumulo for errors.", range));
        throw e;
      }
    }
  }

  public static void consume(Iterable<?> iterable) {
    Iterator<?> itr = iterable.iterator();
    while (itr.hasNext()) {
      itr.next();
    }
  }

  public static Range pickRange(TableOperations tops, String table, Random r) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    ArrayList<Text> splits = Lists.newArrayList(tops.listSplits(table));
    if (splits.isEmpty()) {
      return new Range();
    } else {
      int index = r.nextInt(splits.size());
      Text endRow = splits.get(index);
      Text startRow = index == 0 ? null : splits.get(index - 1);
      return new Range(startRow, false, endRow, true);
    }
  }

  /*
   * These interfaces + implementations are used to determine how many times the scanner should look up a random tablet and scan it.
   */
  static interface LoopControl {
    public boolean keepScanning();
  }

  // Does a finite number of iterations
  static class IterativeLoopControl implements LoopControl {
    private final int max;
    private int current;

    public IterativeLoopControl(int max) {
      this.max = max;
      this.current = 0;
    }

    public boolean keepScanning() {
      if (current < max) {
        ++current;
        return true;
      } else {
        return false;
      }
    }
  }

  // Does an infinite number of iterations
  static class ContinuousLoopControl implements LoopControl {
    public boolean keepScanning() {
      return true;
    }
  }
}
