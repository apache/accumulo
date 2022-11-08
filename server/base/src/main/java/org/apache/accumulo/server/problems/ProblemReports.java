/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.iterators.SortedKeyIterator;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProblemReports implements Iterable<ProblemReport> {

  private static final Logger log = LoggerFactory.getLogger(ProblemReports.class);

  private final LRUMap<ProblemReport,Long> problemReports = new LRUMap<>(1000);

  /**
   * use a thread pool so that reporting a problem never blocks
   *
   * make the thread pool use a bounded queue to avoid the case where problem reports are not being
   * processed because the whole system is in a really bad state (like HDFS is down) and everything
   * is reporting lots of problems, but problem reports can not be processed
   */
  private ExecutorService reportExecutor = ThreadPools.getServerThreadPools().createThreadPool(0, 1,
      60, TimeUnit.SECONDS, "acu-problem-reporter", new LinkedBlockingQueue<>(500), false);

  private final ServerContext context;

  public ProblemReports(ServerContext context) {
    this.context = context;
  }

  public void report(final ProblemReport pr) {

    synchronized (problemReports) {
      if (problemReports.containsKey(pr)) {
        return;
      }

      problemReports.put(pr, System.currentTimeMillis());
    }

    Runnable r = () -> {

      log.debug("Filing problem report {} {} {}", pr.getTableId(), pr.getProblemType(),
          pr.getResource());

      try {
        if (isMeta(pr.getTableId())) {
          // file report in zookeeper
          pr.saveToZooKeeper(context);
        } else {
          // file report in metadata table
          pr.saveToMetadataTable(context);
        }
      } catch (Exception e) {
        log.error("Failed to file problem report " + pr.getTableId() + " " + pr.getProblemType()
            + " " + pr.getResource(), e);
      }
    };

    try {
      reportExecutor.execute(r);
    } catch (RejectedExecutionException ree) {
      log.error("Failed to report problem {} {} {} {}", pr.getTableId(), pr.getProblemType(),
          pr.getResource(), ree.getMessage());
    }

  }

  public void printProblems() {
    for (ProblemReport pr : this) {
      System.out.println(pr.getTableId() + " " + pr.getProblemType() + " " + pr.getResource() + " "
          + pr.getException());
    }
  }

  public void deleteProblemReport(TableId table, ProblemType pType, String resource) {
    final ProblemReport pr = new ProblemReport(table, pType, resource, null);

    Runnable r = () -> {
      try {
        if (isMeta(pr.getTableId())) {
          // file report in zookeeper
          pr.removeFromZooKeeper(context);
        } else {
          // file report in metadata table
          pr.removeFromMetadataTable(context);
        }
      } catch (Exception e) {
        log.error("Failed to delete problem report {} {} {}", pr.getTableId(), pr.getProblemType(),
            pr.getResource(), e);
      }
    };

    try {
      reportExecutor.execute(r);
    } catch (RejectedExecutionException ree) {
      log.error("Failed to delete problem report {} {} {} {}", pr.getTableId(), pr.getProblemType(),
          pr.getResource(), ree.getMessage());
    }
  }

  private static ProblemReports instance;

  public void deleteProblemReports(TableId table) throws Exception {

    if (isMeta(table)) {
      Iterator<ProblemReport> pri = iterator(table);
      while (pri.hasNext()) {
        pri.next().removeFromZooKeeper(context);
      }
      return;
    }

    Scanner scanner = context.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.addScanIterator(new IteratorSetting(1, "keys-only", SortedKeyIterator.class));

    scanner.setRange(new Range(new Text("~err_" + table)));

    Mutation delMut = new Mutation(new Text("~err_" + table));

    boolean hasProblems = false;
    for (Entry<Key,Value> entry : scanner) {
      hasProblems = true;
      delMut.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier());
    }

    if (hasProblems) {
      MetadataTableUtil.getMetadataTable(context).update(delMut);
    }
  }

  private static boolean isMeta(TableId tableId) {
    return tableId.equals(MetadataTable.ID) || tableId.equals(RootTable.ID);
  }

  public Iterator<ProblemReport> iterator(final TableId table) {
    try {

      return new Iterator<>() {

        ZooReaderWriter zoo = context.getZooReaderWriter();
        private int iter1Count = 0;
        private Iterator<String> iter1;

        private Iterator<String> getIter1() {
          if (iter1 == null) {
            try {
              List<String> children;
              if (table == null || isMeta(table)) {
                children = zoo.getChildren(context.getZooKeeperRoot() + Constants.ZPROBLEMS);
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
                Scanner scanner = context.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
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
          return getIter2().hasNext();
        }

        @Override
        public ProblemReport next() {
          try {
            if (getIter1().hasNext()) {
              iter1Count++;
              return ProblemReport.decodeZooKeeperEntry(context, getIter1().next());
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

  public static synchronized ProblemReports getInstance(ServerContext context) {
    if (instance == null) {
      instance = new ProblemReports(context);
    }

    return instance;
  }

  public static void main(String[] args) {
    var context = new ServerContext(SiteConfiguration.auto());
    getInstance(context).printProblems();
  }

  public Map<TableId,Map<ProblemType,Integer>> summarize() {

    TreeMap<TableId,Map<ProblemType,Integer>> summary = new TreeMap<>();

    for (ProblemReport pr : this) {
      Map<ProblemType,Integer> tableProblems = summary.get(pr.getTableId());
      if (tableProblems == null) {
        tableProblems = new EnumMap<>(ProblemType.class);
        summary.put(pr.getTableId(), tableProblems);
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
