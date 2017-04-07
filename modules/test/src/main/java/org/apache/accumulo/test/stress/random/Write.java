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

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;

public class Write {

  public static void main(String[] args) throws Exception {
    WriteOptions opts = new WriteOptions();
    BatchWriterOpts batch_writer_opts = new BatchWriterOpts();
    opts.parseArgs(Write.class.getName(), args, batch_writer_opts);

    opts.check();

    Connector c = opts.getConnector();

    if (opts.clear_table && c.tableOperations().exists(opts.getTableName())) {
      try {
        c.tableOperations().delete(opts.getTableName());
      } catch (TableNotFoundException e) {
        System.err.println("Couldn't delete the table because it doesn't exist any more.");
      }
    }

    if (!c.tableOperations().exists(opts.getTableName())) {
      try {
        c.tableOperations().create(opts.getTableName());
      } catch (TableExistsException e) {
        System.err.println("Couldn't create table ourselves, but that's ok. Continuing.");
      }
    }

    long writeDelay = opts.write_delay;
    if (writeDelay < 0) {
      writeDelay = 0;
    }

    DataWriter dw = new DataWriter(c.createBatchWriter(opts.getTableName(), batch_writer_opts.getBatchWriterConfig()), new RandomMutations(
    // rows
        new RandomByteArrays(new RandomWithinRange(opts.row_seed, opts.rowMin(), opts.rowMax())),
        // cfs
        new RandomByteArrays(new RandomWithinRange(opts.cf_seed, opts.cfMin(), opts.cfMax())),
        // cqs
        new RandomByteArrays(new RandomWithinRange(opts.cq_seed, opts.cqMin(), opts.cqMax())),
        // vals
        new RandomByteArrays(new RandomWithinRange(opts.value_seed, opts.valueMin(), opts.valueMax())),
        // number of cells per row
        new RandomWithinRange(opts.row_width_seed, opts.rowWidthMin(), opts.rowWidthMax()),
        // max cells per mutation
        opts.max_cells_per_mutation));

    while (true) {
      dw.next();
      if (writeDelay > 0) {
        Thread.sleep(writeDelay);
      }
    }
  }
}
