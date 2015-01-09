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
package org.apache.accumulo.tserver;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.problems.ProblemReport;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.problems.ProblemType;
import org.apache.accumulo.tserver.Tablet.MinorCompactionReason;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class MinorCompactor extends Compactor {

  private static final Logger log = Logger.getLogger(MinorCompactor.class);

  private static final Map<FileRef,DataFileValue> EMPTY_MAP = Collections.emptyMap();

  private static Map<FileRef,DataFileValue> toFileMap(FileRef mergeFile, DataFileValue dfv) {
    if (mergeFile == null)
      return EMPTY_MAP;

    return Collections.singletonMap(mergeFile, dfv);
  }

  MinorCompactor(Configuration conf, VolumeManager fs, InMemoryMap imm, FileRef mergeFile, DataFileValue dfv, FileRef outputFile,
      TableConfiguration acuTableConf, KeyExtent extent, MinorCompactionReason mincReason) {
    super(conf, fs, toFileMap(mergeFile, dfv), imm, outputFile, true, acuTableConf, extent, new CompactionEnv() {

      @Override
      public boolean isCompactionEnabled() {
        return true;
      }

      @Override
      public IteratorScope getIteratorScope() {
        return IteratorScope.minc;
      }
    });

    super.mincReason = mincReason;
  }

  private boolean isTableDeleting() {
    try {
      return Tables.getTableState(HdfsZooInstance.getInstance(), extent.getTableId().toString()) == TableState.DELETING;
    } catch (Exception e) {
      log.warn("Failed to determine if table " + extent.getTableId() + " was deleting ", e);
      return false; // can not get positive confirmation that its deleting.
    }
  }

  @Override
  public CompactionStats call() {
    log.debug("Begin minor compaction " + getOutputFile() + " " + getExtent());

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

          // log.debug(String.format("MinC %,d recs in | %,d recs out | %,d recs/sec | %6.3f secs | %,d bytes ",map.size(), entriesCompacted,
          // (int)(map.size()/((t2 - t1)/1000.0)), (t2 - t1)/1000.0, estimatedSizeInBytes()));

          if (reportedProblem) {
            ProblemReports.getInstance().deleteProblemReport(getExtent().getTableId().toString(), ProblemType.FILE_WRITE, getOutputFile());
          }

          return ret;
        } catch (IOException e) {
          log.warn("MinC failed (" + e.getMessage() + ") to create " + getOutputFile() + " retrying ...");
          ProblemReports.getInstance().report(new ProblemReport(getExtent().getTableId().toString(), ProblemType.FILE_WRITE, getOutputFile(), e));
          reportedProblem = true;
        } catch (RuntimeException e) {
          // if this is coming from a user iterator, it is possible that the user could change the iterator config and that the
          // minor compaction would succeed
          log.warn("MinC failed (" + e.getMessage() + ") to create " + getOutputFile() + " retrying ...", e);
          ProblemReports.getInstance().report(new ProblemReport(getExtent().getTableId().toString(), ProblemType.FILE_WRITE, getOutputFile(), e));
          reportedProblem = true;
        } catch (CompactionCanceledException e) {
          throw new IllegalStateException(e);
        }

        Random random = new Random();

        int sleep = sleepTime + random.nextInt(sleepTime);
        log.debug("MinC failed sleeping " + sleep + " ms before retrying");
        UtilWaitThread.sleep(sleep);
        sleepTime = (int) Math.round(Math.min(maxSleepTime, sleepTime * growthFactor));

        // clean up
        try {
          if (getFileSystem().exists(new Path(getOutputFile()))) {
            getFileSystem().deleteRecursively(new Path(getOutputFile()));
          }
        } catch (IOException e) {
          log.warn("Failed to delete failed MinC file " + getOutputFile() + " " + e.getMessage());
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
