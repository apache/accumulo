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
package org.apache.accumulo.examples.simple.client;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;

import com.beust.jcommander.Parameter;

/**
 * Simple example for writing random data in sequential order to Accumulo. See docs/examples/README.batch for instructions.
 */
public class SequentialBatchWriter {

  static class Opts extends ClientOnRequiredTable {
    @Parameter(names = "--start")
    long start = 0;
    @Parameter(names = "--num", required = true)
    long num = 0;
    @Parameter(names = "--size", required = true, description = "size of the value to write")
    int valueSize = 0;
    @Parameter(names = "--vis", converter = VisibilityConverter.class)
    ColumnVisibility vis = new ColumnVisibility();
  }

  /**
   * Writes a specified number of entries to Accumulo using a {@link BatchWriter}. The rows of the entries will be sequential starting at a specified number.
   * The column families will be "foo" and column qualifiers will be "1". The values will be random byte arrays of a specified size.
   */
  public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, MutationsRejectedException {
    Opts opts = new Opts();
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    opts.parseArgs(SequentialBatchWriter.class.getName(), args, bwOpts);
    Connector connector = opts.getConnector();
    BatchWriter bw = connector.createBatchWriter(opts.tableName, bwOpts.getBatchWriterConfig());

    long end = opts.start + opts.num;

    for (long i = opts.start; i < end; i++) {
      Mutation m = RandomBatchWriter.createMutation(i, opts.valueSize, opts.vis);
      bw.addMutation(m);
    }

    bw.close();
  }
}
