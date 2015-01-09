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
package org.apache.accumulo.server.problems;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyIterator;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.LoggingRunnable;
import org.apache.accumulo.core.util.NamingThreadFactory;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class ProblemReports implements Iterable<ProblemReport> {

  private static final Logger log = Logger.getLogger(ProblemReports.class);

  private final LRUMap problemReports = new LRUMap(1000);

  /*
   * use a thread pool so that reporting a problem never blocks
   *
   * make the thread pool use a bounded queue to avoid the case where problem reports are not being processed because the whole system is in a really bad state
   * (like HDFS is down) and everything is reporting lots of problems, but problem reports can not be processed
   */
  private ExecutorService reportExecutor = new ThreadPoolExecutor(0, 1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(500), new NamingThreadFactory(
      "acu-problem-reporter"));

  public void report(final ProblemReport pr) {

    synchronized (problemReports) {
      if (problemReports.containsKey(pr)) {
        return;
      }

      problemReports.put(pr, System.currentTimeMillis());
    }

    Runnable r = new Runnable() {

      @Override
      public void run() {

        log.debug("Filing problem report " + pr.getTableName() + " " + pr.getProblemType() + " " + pr.getResource());

        try {
          if (isMeta(pr.getTableName())) {
            // file report in zookeeper
            pr.saveToZooKeeper();
          } else {
            // file report in metadata table
            pr.saveToMetadataTable();
          }
        } catch (Exception e) {
          log.error("Failed to file problem report " + pr.getTableName() + " " + pr.getProblemType() + " " + pr.getResource(), e);
        }
      }

    };

    try {
      reportExecutor.execute(new LoggingRunnable(log, r));
    } catch (RejectedExecutionException ree) {
      log.error("Failed to report problem " + pr.getTableName() + " " + pr.getProblemType() + " " + pr.getResource() + "  " + ree.getMessage());
    }

  }

  public void printProblems() throws Exception {
    for (ProblemReport pr : this) {
      System.out.println(pr.getTableName() + " " + pr.getProblemType() + " " + pr.getResource() + " " + pr.getException());
    }
  }

  public void deleteProblemReport(String table, ProblemType pType, String resource) {
    final ProblemReport pr = new ProblemReport(table, pType, resource, null);

    Runnable r = new Runnable() {

      @Override
      public void run() {
        try {
          if (isMeta(pr.getTableName())) {
            // file report in zookeeper
            pr.removeFromZooKeeper();
          } else {
            // file report in metadata table
            pr.removeFromMetadataTable();
          }
        } catch (Exception e) {
          log.error("Failed to delete problem report " + pr.getTableName() + " " + pr.getProblemType() + " " + pr.getResource(), e);
        }
      }
    };

    try {
      reportExecutor.execute(new LoggingRunnable(log, r));
    } catch (RejectedExecutionException ree) {
      log.error("Failed to delete problem report " + pr.getTableName() + " " + pr.getProblemType() + " " + pr.getResource() + "  " + ree.getMessage());
    }
  }

  private static ProblemReports instance;

  public void deleteProblemReports(String table) throws Exception {

    if (isMeta(table)) {
      Iterator<ProblemReport> pri = iterator(table);
      while (pri.hasNext()) {
        pri.next().removeFromZooKeeper();
      }
      return;
    }

    Connector connector = HdfsZooInstance.getInstance().getConnector(SystemCredentials.get().getPrincipal(), SystemCredentials.get().getToken());
    Scanner scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.addScanIterator(new IteratorSetting(1, "keys-only", SortedKeyIterator.class));

    scanner.setRange(new Range(new Text("~err_" + table)));

    Mutation delMut = new Mutation(new Text("~err_" + table));

    boolean hasProblems = false;
    for (Entry<Key,Value> entry : scanner) {
      hasProblems = true;
      delMut.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier());
    }

    if (hasProblems)
      MetadataTableUtil.getMetadataTable(SystemCredentials.get()).update(delMut);
  }

  private static boolean isMeta(String tableId) {
    return tableId.equals(MetadataTable.ID) || tableId.equals(RootTable.ID);
  }

  public Iterator<ProblemReport> iterator(final String table) {
    try {

      return new Iterator<ProblemReport>() {

        IZooReaderWriter zoo = ZooReaderWriter.getInstance();
        private int iter1Count = 0;
        private Iterator<String> iter1;

        private Iterator<String> getIter1() {
          if (iter1 == null) {
            try {
              List<String> children;
              if (table == null || isMeta(table)) {
                children = zoo.getChildren(ZooUtil.getRoot(HdfsZooInstance.getInstance()) + Constants.ZPROBLEMS);
              } else {
                children = Collections.emptyList();
              }
              iter1 = children.iterator();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          return iter1;
        }

        private Iterator<Entry<Key,Value>> iter2;

        private Iterator<Entry<Key,Value>> getIter2() {
          if (iter2 == null) {
            try {
              if ((table == null || !isMeta(table)) && iter1Count == 0) {
                Connector connector = HdfsZooInstance.getInstance().getConnector(SystemCredentials.get().getPrincipal(), SystemCredentials.get().getToken());
                Scanner scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);

                scanner.setTimeout(3, TimeUnit.SECONDS);

                if (table == null) {
                  scanner.setRange(new Range(new Text("~err_"), false, new Text("~err`"), false));
                } else {
                  scanner.setRange(new Range(new Text("~err_" + table)));
                }

                iter2 = scanner.iterator();

              } else {
                Map<Key,Value> m = Collections.emptyMap();
                iter2 = m.entrySet().iterator();
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          return iter2;
        }

        @Override
        public boolean hasNext() {
          if (getIter1().hasNext()) {
            return true;
          }

          if (getIter2().hasNext()) {
            return true;
          }

          return false;
        }

        @Override
        public ProblemReport next() {
          try {
            if (getIter1().hasNext()) {
              iter1Count++;
              return ProblemReport.decodeZooKeeperEntry(getIter1().next());
            }

            if (getIter2().hasNext()) {
              return ProblemReport.decodeMetadataEntry(getIter2().next());
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }

          throw new NoSuchElementException();
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }

      };

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterator<ProblemReport> iterator() {
    return iterator(null);
  }

  public static synchronized ProblemReports getInstance() {
    if (instance == null) {
      instance = new ProblemReports();
    }

    return instance;
  }

  public static void main(String args[]) throws Exception {
    getInstance().printProblems();
  }

  public Map<String,Map<ProblemType,Integer>> summarize() {

    TreeMap<String,Map<ProblemType,Integer>> summary = new TreeMap<String,Map<ProblemType,Integer>>();

    for (ProblemReport pr : this) {
      Map<ProblemType,Integer> tableProblems = summary.get(pr.getTableName());
      if (tableProblems == null) {
        tableProblems = new EnumMap<ProblemType,Integer>(ProblemType.class);
        summary.put(pr.getTableName(), tableProblems);
      }

      Integer count = tableProblems.get(pr.getProblemType());
      if (count == null) {
        count = 0;
      }

      tableProblems.put(pr.getProblemType(), count + 1);
    }

    return summary;
  }

}
