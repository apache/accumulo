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
package org.apache.accumulo.server.util;

import java.util.Iterator;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class RandomWriter {
  
  private static String table_name = "test_write_table";
  private static int num_columns_per_row = 1;
  private static int num_payload_bytes = 1024;
  private static final Logger log = Logger.getLogger(RandomWriter.class);
  
  public static class RandomMutationGenerator implements Iterable<Mutation>, Iterator<Mutation> {
    private long max_mutations;
    private int mutations_so_far = 0;
    private Random r = new Random();
    private static final Logger log = Logger.getLogger(RandomMutationGenerator.class);
    
    public RandomMutationGenerator(long num_mutations) {
      max_mutations = num_mutations;
    }
    
    public boolean hasNext() {
      return mutations_so_far < max_mutations;
    }
    
    public Mutation next() {
      Text row_value = new Text(Long.toString((Math.abs(r.nextLong()) / 177) % 100000000000l));
      Mutation m = new Mutation(row_value);
      for (int column = 0; column < num_columns_per_row; column++) {
        Text column_fam = new Text("col_fam");
        byte[] bytes = new byte[num_payload_bytes];
        r.nextBytes(bytes);
        m.put(column_fam, new Text("" + column), new Value(bytes));
      }
      mutations_so_far++;
      if (mutations_so_far % 1000000 == 0) {
        log.info("Created " + mutations_so_far + " mutations so far");
      }
      return m;
    }
    
    public void remove() {
      mutations_so_far++;
    }
    
    @Override
    public Iterator<Mutation> iterator() {
      return this;
    }
  }
  
  public static void main(String[] args) throws MutationsRejectedException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
    long start = System.currentTimeMillis();
    if (args.length != 3) {
      log.error("Usage: bin/accumulo " + RandomWriter.class.getName() + " <username> <password> <num_mutations_to_write>");
      return;
    }
    log.info("starting at " + start + " for user " + args[0]);
    try {
      Connector connector = HdfsZooInstance.getInstance().getConnector(args[0], args[1].getBytes());
      BatchWriter bw = connector.createBatchWriter(table_name, 50000000l, 10 * 60l, 10);
      long num_mutations = Long.parseLong(args[2]);
      log.info("Writing " + num_mutations + " mutations...");
      bw.addMutations(new RandomMutationGenerator(num_mutations));
      bw.close();
    } catch (MutationsRejectedException e) {
      log.error(e);
      throw e;
    } catch (AccumuloException e) {
      log.error(e);
      throw e;
    } catch (AccumuloSecurityException e) {
      log.error(e);
      throw e;
    } catch (TableNotFoundException e) {
      log.error(e);
      throw e;
    }
    long stop = System.currentTimeMillis();
    
    log.info("stopping at " + stop);
    log.info("elapsed: " + (((double) stop - (double) start) / 1000.0));
  }
  
}
