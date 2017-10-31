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

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.util.SimpleThreadPool;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;

// ACCUMULO-3692
public class BalanceWithOfflineTableIT extends ConfigurableMacBase {

  @Override
  protected int defaultTimeoutSeconds() {
    return 30;
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {}

  @Test
  public void test() throws Exception {
    final String tableNames[] = getUniqueNames(2);
    final String tableName = tableNames[0];
    // create a table with a bunch of splits

    final Connector c = getConnector();
    log.info("Creating table {}", tableName);
    c.tableOperations().create(tableName);
    final SortedSet<Text> splits = new TreeSet<>();
    for (String split : "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z".split(",")) {
      splits.add(new Text(split));
    }
    log.info("Splitting table {}", tableName);
    c.tableOperations().addSplits(tableName, splits);
    log.info("Balancing");
    c.instanceOperations().waitForBalance();
    log.info("Balanced");

    // create a new table which will unbalance the cluster
    final String table2 = tableNames[1];
    log.info("Creating table {}", table2);
    c.tableOperations().create(table2);
    log.info("Creating splits {}", table2);
    c.tableOperations().addSplits(table2, splits);

    // offline the table, hopefully while there are some migrations going on
    log.info("Offlining {}", table2);
    c.tableOperations().offline(table2, true);
    log.info("Offlined {}", table2);

    log.info("Waiting for balance");

    SimpleThreadPool pool = new SimpleThreadPool(1, "waitForBalance");
    Future<Boolean> wait = pool.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        c.instanceOperations().waitForBalance();
        return true;
      }
    });
    wait.get(20, TimeUnit.SECONDS);
    log.info("Balance succeeded with an offline table");
  }

}
