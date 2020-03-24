/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.tablet;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.problems.ProblemReport;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.problems.ProblemType;
import org.apache.accumulo.tserver.InMemoryMap;
import org.apache.accumulo.tserver.MinorCompactionReason;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinorCompactor extends Compactor {

  private static final Logger log = LoggerFactory.getLogger(MinorCompactor.class);

  private static final Map<StoredTabletFile,DataFileValue> EMPTY_MAP = Collections.emptyMap();

  private static Map<StoredTabletFile,DataFileValue> toFileMap(StoredTabletFile mergeFile,
      DataFileValue dfv) {
    if (mergeFile == null)
      return EMPTY_MAP;

    return Collections.singletonMap(mergeFile, dfv);
  }

  private final TabletServer tabletServer;

  public MinorCompactor(TabletServer tabletServer, Tablet tablet, InMemoryMap imm,
      StoredTabletFile mergeFile, DataFileValue dfv, TabletFile outputFile,
      MinorCompactionReason mincReason, TableConfiguration tableConfig) {
    super(tabletServer.getContext(), tablet, toFileMap(mergeFile, dfv), imm, outputFile, true,
        new CompactionEnv() {

          @Override
          public boolean isCompactionEnabled() {
            return true;
          }

          @Override
          public IteratorScope getIteratorScope() {
            return IteratorScope.minc;
          }

          @Override
          public RateLimiter getReadLimiter() {
            return null;
          }

          @Override
          public RateLimiter getWriteLimiter() {
            return null;
          }
        }, Collections.emptyList(), mincReason.ordinal(), tableConfig);
    this.tabletServer = tabletServer;
  }

  private boolean isTableDeleting() {
    try {
      return Tables.getTableState(tabletServer.getContext(), extent.getTableId())
          == TableState.DELETING;
    } catch (Exception e) {
      log.warn("Failed to determine if table " + extent.getTableId() + " was deleting ", e);
      return false; // can not get positive confirmation that its deleting.
    }
  }

  @Override
  protected Map<String,Set<ByteSequence>> getLocalityGroups(AccumuloConfiguration acuTableConf)
      throws IOException {
    return LocalityGroupUtil.getLocalityGroupsIgnoringErrors(acuTableConf, extent.getTableId());
  }

  @Override
  public CompactionStats call() {
    final String outputFileName = getOutputFile();
    log.trace("Begin minor compaction {} {}", outputFileName, getExtent());

    // output to new MapFile with a temporary name
    int sleepTime = 100;
    double growthFactor = 4;
    int maxSleepTime = 1000 * 60 * 3; // 3 minutes
    boolean reportedProblem = false;

    runningCompactions.add(this);
    try {
      do {
        try {
          CompactionStats ret = super.call();

          // log.debug(String.format("MinC %,d recs in | %,d recs out | %,d recs/sec | %6.3f secs |
          // %,d bytes ",map.size(), entriesCompacted,
          // (int)(map.size()/((t2 - t1)/1000.0)), (t2 - t1)/1000.0, estimatedSizeInBytes()));

          if (reportedProblem) {
            ProblemReports.getInstance(tabletServer.getContext()).deleteProblemReport(
                getExtent().getTableId(), ProblemType.FILE_WRITE, outputFileName);
          }

          return ret;
        } catch (IOException | UnsatisfiedLinkError e) {
          log.warn("MinC failed ({}) to create {} retrying ...", e.getMessage(), outputFileName);
          ProblemReports.getInstance(tabletServer.getContext()).report(new ProblemReport(
              getExtent().getTableId(), ProblemType.FILE_WRITE, outputFileName, e));
          reportedProblem = true;
        } catch (RuntimeException e) {
          // if this is coming from a user iterator, it is possible that the user could change the
          // iterator config and that the
          // minor compaction would succeed
          log.warn("MinC failed ({}) to create {} retrying ...", e.getMessage(), outputFileName, e);
          ProblemReports.getInstance(tabletServer.getContext()).report(new ProblemReport(
              getExtent().getTableId(), ProblemType.FILE_WRITE, outputFileName, e));
          reportedProblem = true;
        } catch (CompactionCanceledException e) {
          throw new IllegalStateException(e);
        }

        Random random = new SecureRandom();

        int sleep = sleepTime + random.nextInt(sleepTime);
        log.debug("MinC failed sleeping {} ms before retrying", sleep);
        sleepUninterruptibly(sleep, TimeUnit.MILLISECONDS);
        sleepTime = (int) Math.round(Math.min(maxSleepTime, sleepTime * growthFactor));

        // clean up
        try {
          if (getFileSystem().exists(new Path(outputFileName))) {
            getFileSystem().deleteRecursively(new Path(outputFileName));
          }
        } catch (IOException e) {
          log.warn("Failed to delete failed MinC file {} {}", outputFileName, e.getMessage());
        }

        if (isTableDeleting())
          return new CompactionStats(0, 0);

      } while (true);
    } finally {
      thread = null;
      runningCompactions.remove(this);
    }
  }

}
