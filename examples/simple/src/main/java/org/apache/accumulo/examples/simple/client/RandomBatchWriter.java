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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;

/**
 * Simple example for writing random data to Accumulo. See docs/examples/README.batch for instructions.
 *
 * The rows of the entries will be randomly generated numbers between a specified min and max (prefixed by "row_"). The column families will be "foo" and column
 * qualifiers will be "1". The values will be random byte arrays of a specified size.
 */
public class RandomBatchWriter {

  /**
   * Creates a random byte array of specified size using the specified seed.
   *
   * @param rowid
   *          the seed to use for the random number generator
   * @param dataSize
   *          the size of the array
   * @return a random byte array
   */
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

  /**
   * Creates a mutation on a specified row with column family "foo", column qualifier "1", specified visibility, and a random value of specified size.
   *
   * @param rowid
   *          the row of the mutation
   * @param dataSize
   *          the size of the random value
   * @param visibility
   *          the visibility of the entry to insert
   * @return a mutation
   */
  public static Mutation createMutation(long rowid, int dataSize, ColumnVisibility visibility) {
    Text row = new Text(String.format("row_%010d", rowid));

    Mutation m = new Mutation(row);

    // create a random value that is a function of the
    // row id for verification purposes
    byte value[] = createValue(rowid, dataSize);

    m.put(new Text("foo"), new Text("1"), visibility, new Value(value));

    return m;
  }

  static class Opts extends ClientOnRequiredTable {
    @Parameter(names = "--num", required = true)
    int num = 0;
    @Parameter(names = "--min")
    long min = 0;
    @Parameter(names = "--max")
    long max = Long.MAX_VALUE;
    @Parameter(names = "--size", required = true, description = "size of the value to write")
    int size = 0;
    @Parameter(names = "--vis", converter = VisibilityConverter.class)
    ColumnVisibility visiblity = new ColumnVisibility("");
    @Parameter(names = "--seed", description = "seed for pseudo-random number generator")
    Long seed = null;
  }

  public static long abs(long l) {
    l = Math.abs(l); // abs(Long.MIN_VALUE) == Long.MIN_VALUE...
    if (l < 0)
      return 0;
    return l;
  }

  /**
   * Writes a specified number of entries to Accumulo using a {@link BatchWriter}.
   */
  public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    Opts opts = new Opts();
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    opts.parseArgs(RandomBatchWriter.class.getName(), args, bwOpts);
    if ((opts.max - opts.min) < 1L * opts.num) { // right-side multiplied by 1L to convert to long in a way that doesn't trigger FindBugs
      System.err.println(String.format("You must specify a min and a max that allow for at least num possible values. "
          + "For example, you requested %d rows, but a min of %d and a max of %d (exclusive), which only allows for %d rows.", opts.num, opts.min, opts.max,
          (opts.max - opts.min)));
      System.exit(1);
    }
    Random r;
    if (opts.seed == null)
      r = new Random();
    else {
      r = new Random(opts.seed);
    }
    Connector connector = opts.getConnector();
    BatchWriter bw = connector.createBatchWriter(opts.tableName, bwOpts.getBatchWriterConfig());

    // reuse the ColumnVisibility object to improve performance
    ColumnVisibility cv = opts.visiblity;

    // Generate num unique row ids in the given range
    HashSet<Long> rowids = new HashSet<Long>(opts.num);
    while (rowids.size() < opts.num) {
      rowids.add((abs(r.nextLong()) % (opts.max - opts.min)) + opts.min);
    }
    for (long rowid : rowids) {
      Mutation m = createMutation(rowid, opts.size, cv);
      bw.addMutation(m);
    }

    try {
      bw.close();
    } catch (MutationsRejectedException e) {
      if (e.getAuthorizationFailuresMap().size() > 0) {
        HashMap<String,Set<SecurityErrorCode>> tables = new HashMap<String,Set<SecurityErrorCode>>();
        for (Entry<KeyExtent,Set<SecurityErrorCode>> ke : e.getAuthorizationFailuresMap().entrySet()) {
          Set<SecurityErrorCode> secCodes = tables.get(ke.getKey().getTableId().toString());
          if (secCodes == null) {
            secCodes = new HashSet<SecurityErrorCode>();
            tables.put(ke.getKey().getTableId().toString(), secCodes);
          }
          secCodes.addAll(ke.getValue());
        }
        System.err.println("ERROR : Not authorized to write to tables : " + tables);
      }

      if (e.getConstraintViolationSummaries().size() > 0) {
        System.err.println("ERROR : Constraint violations occurred : " + e.getConstraintViolationSummaries());
      }
      System.exit(1);
    }
  }
}
