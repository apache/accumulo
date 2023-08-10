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
package org.apache.accumulo.tserver.tablet;

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.server.compaction.CompactionStats;
import org.apache.accumulo.server.compaction.FileCompactor;
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

public class MinorCompactor extends FileCompactor {

  private static final SecureRandom random = new SecureRandom();
  private static final Logger log = LoggerFactory.getLogger(MinorCompactor.class);

  private final TabletServer tabletServer;
  private final MinorCompactionReason mincReason;

  public MinorCompactor(TabletServer tabletServer, Tablet tablet, InMemoryMap imm,
      TabletFile outputFile, MinorCompactionReason mincReason, TableConfiguration tableConfig) {
    super(tabletServer.getContext(), tablet.getExtent(), Collections.emptyMap(), outputFile, true,
        new MinCEnv(mincReason, imm.compactionIterator()), Collections.emptyList(), tableConfig,
        tableConfig.getCryptoService());
    this.tabletServer = tabletServer;
    this.mincReason = mincReason;
  }

  private boolean isTableDeleting() {
    try {
      return tabletServer.getContext().getTableState(extent.tableId()) == TableState.DELETING;
    } catch (Exception e) {
      log.warn("Failed to determine if table " + extent.tableId() + " was deleting ", e);
      return false; // can not get positive confirmation that its deleting.
    }
  }

  @Override
  protected Map<String,Set<ByteSequence>> getLocalityGroups(AccumuloConfiguration acuTableConf)
      throws IOException {
    return LocalityGroupUtil.getLocalityGroupsIgnoringErrors(acuTableConf, extent.tableId());
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
    int retryCounter = 0;

    runningCompactions.add(this);
    try {
      do {
        try {
          CompactionStats ret = super.call();

          // log.debug(String.format("MinC %,d recs in | %,d recs out | %,d recs/sec | %6.3f secs |
          // %,d bytes ",map.size(), entriesCompacted,
          // (int)(map.size()/((t2 - t1)/1000.0)), (t2 - t1)/1000.0, estimatedSizeInBytes()));

          if (reportedProblem) {
            ProblemReports.getInstance(tabletServer.getContext())
                .deleteProblemReport(getExtent().tableId(), ProblemType.FILE_WRITE, outputFileName);
          }

          return ret;
        } catch (IOException | UnsatisfiedLinkError e) {
          log.warn("MinC failed ({}) to create {} retrying ...", e.getMessage(), outputFileName);
          ProblemReports.getInstance(tabletServer.getContext()).report(
              new ProblemReport(getExtent().tableId(), ProblemType.FILE_WRITE, outputFileName, e));
          reportedProblem = true;
        } catch (RuntimeException | NoClassDefFoundError e) {
          // if this is coming from a user iterator, it is possible that the user could change the
          // iterator config and that the minor compaction would succeed
          // If the minor compaction stalls for too long during recovery, it can interfere with
          // other tables loading
          // Throw exception if this happens so assignments can be rescheduled.
          ProblemReports.getInstance(tabletServer.getContext()).report(
              new ProblemReport(getExtent().tableId(), ProblemType.FILE_WRITE, outputFileName, e));
          if (retryCounter >= 4 && mincReason.equals(MinorCompactionReason.RECOVERY)) {
            log.warn(
                "MinC ({}) is stuck for too long during recovery, throwing error to reschedule.",
                getExtent(), e);
            throw new RuntimeException(e);
          }
          log.warn("MinC failed ({}) to create {} retrying ...", e.getMessage(), outputFileName, e);
          reportedProblem = true;
          retryCounter++;
        } catch (CompactionCanceledException e) {
          throw new IllegalStateException(e);
        }

        int sleep = sleepTime + random.nextInt(sleepTime);
        log.debug("MinC failed sleeping {} ms before retrying", sleep);
        sleepUninterruptibly(sleep, TimeUnit.MILLISECONDS);
        sleepTime = (int) Math.round(Math.min(maxSleepTime, sleepTime * growthFactor));

        // clean up
        try {
          if (getVolumeManager().exists(new Path(outputFileName))) {
            getVolumeManager().deleteRecursively(new Path(outputFileName));
          }
        } catch (IOException e) {
          log.warn("Failed to delete failed MinC file {} {}", outputFileName, e.getMessage());
        }

        if (isTableDeleting()) {
          return new CompactionStats(0, 0);
        }

      } while (true);
    } finally {
      thread = null;
      runningCompactions.remove(this);
    }
  }

}
