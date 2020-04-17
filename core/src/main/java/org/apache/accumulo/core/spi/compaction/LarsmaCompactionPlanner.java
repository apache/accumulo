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
package org.apache.accumulo.core.spi.compaction;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;

/**
 * Finds the largest continuous set of small files that meet the compaction ratio and do not prevent
 * future compactions.
 *
 * <p>
 * The following configuration options are supported. Replace {@code <service>} with the name of the
 * compaction service you are configuring.
 *
 * <ul>
 * <li>{@code tserver.compaction.service.<service>.opts.executors} This is a json array of objects
 * where each object has the fields: name, maxSize, and numThreads. The maxSize field determine the
 * maximum size of compaction that will run on an executor. The maxSize field can have a suffix of
 * K,M,G for kilobytes, megabytes, or gigabytes. One executor can have no max size and it will run
 * everything that is too large for the other executors. If all executors have a max size, then
 * system compactions will only run for compactions smaller than the largest max size. User, chop,
 * and selector compactions will always run, even if there is no executor for their size. These
 * compaction will run on the executor with the largest max size. The value for this field should
 * look like
 * {@code [{"name":"executor1","maxSize":"100M","numThreads":3},{"name":"executor2","maxSize":"500M","numThreads":3},{"executor3":"huge","numThreads":3}]}.
 * This configuration would run compactions less than 100M on executor1, compactions less than 500M
 * on executor2, and all other on executor3.
 * <li>{@code tserver.compaction.service.<service>.opts.maxOpen} This determines the maximum number
 * of files that will be included in a single compaction.
 * </ul>
 *
 * @since 2.1.0
 * @see org.apache.accumulo.core.spi.compaction
 */

public class LarsmaCompactionPlanner implements CompactionPlanner {

  private static Logger log = LoggerFactory.getLogger(LarsmaCompactionPlanner.class);

  public static class ExecutorConfig {
    String name;
    String maxSize;
    int numThreads;
  }

  private static class Executor {
    final CompactionExecutorId ceid;
    final Long maxSize;

    public Executor(CompactionExecutorId ceid, Long maxSize) {
      Preconditions.checkArgument(maxSize == null || maxSize > 0);
      this.ceid = Objects.requireNonNull(ceid);
      this.maxSize = maxSize;
    }

    Long getMaxSize() {
      return maxSize;
    }
  }

  private List<Executor> executors;
  private int maxFilesToCompact;

  @Override
  public void init(InitParameters params) {
    ExecutorConfig[] execConfigs =
        new Gson().fromJson(params.getOptions().get("executors"), ExecutorConfig[].class);

    List<Executor> tmpExec = new ArrayList<>();

    for (ExecutorConfig executorConfig : execConfigs) {
      var ceid = params.getExecutorManager().createExecutor(executorConfig.name,
          executorConfig.numThreads);
      Long maxSize = executorConfig.maxSize == null ? null
          : ConfigurationTypeHelper.getFixedMemoryAsBytes(executorConfig.maxSize);
      tmpExec.add(new Executor(ceid, maxSize));
    }

    Collections.sort(tmpExec, Comparator.comparing(Executor::getMaxSize,
        Comparator.nullsLast(Comparator.naturalOrder())));

    executors = List.copyOf(tmpExec);

    if (executors.stream().filter(e -> e.getMaxSize() == null).count() > 1) {
      throw new IllegalArgumentException(
          "Can only have one executor w/o a maxSize. " + params.getOptions().get("executors"));
    }

    String fqo = params.getFullyQualifiedOption("maxFilesPerCompaction");

    if (!params.getServiceEnvironment().getConfiguration().isSet(fqo)
        && params.getServiceEnvironment().getConfiguration()
            .isSet(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey())) {
      log.warn("The property " + Property.TSERV_MAJC_THREAD_MAXOPEN.getKey()
          + " was set, it is deperecated.  Set the " + fqo + " option instead.");
      this.maxFilesToCompact =
          this.maxFilesToCompact = Integer.parseInt(params.getServiceEnvironment()
              .getConfiguration().get(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey()));
    } else {
      this.maxFilesToCompact = Integer.parseInt(params.getOptions().getOrDefault("maxOpen", "30"));
    }
  }

  @Override
  public CompactionPlan makePlan(PlanningParameters params) {
    try {

      if (params.getCandidates().isEmpty()) {
        // TODO should not even be called in this case
        return new CompactionPlan();
      }

      Set<CompactableFile> filesCopy = new HashSet<>(params.getCandidates());

      long maxSizeToCompact = getMaxSizeToCompact(params.getKind());

      Collection<CompactableFile> group;
      if (params.getRunningCompactions().isEmpty()) {
        group = findMapFilesToCompact(filesCopy, params.getRatio(), maxFilesToCompact,
            maxSizeToCompact);
      } else {
        // This code determines if once the files compacting finish would they be included in a
        // compaction with the files smaller than them? If so, then wait for the running compaction
        // to complete.

        // The set of files running compactions may produce
        var expectedFiles = getExpected(params.getRunningCompactions());

        if (!Collections.disjoint(filesCopy, expectedFiles)) {
          throw new AssertionError();
        }

        filesCopy.addAll(expectedFiles);

        group = findMapFilesToCompact(filesCopy, params.getRatio(), maxFilesToCompact,
            maxSizeToCompact);

        if (!Collections.disjoint(group, expectedFiles)) {
          // file produced by running compaction will eventually compact with existing files, so
          // wait.
          group = Set.of();
        }
      }

      if (group.isEmpty()
          && (params.getKind() == CompactionKind.USER
              || params.getKind() == CompactionKind.SELECTOR)
          && params.getRunningCompactions().stream()
              .filter(job -> job.getKind() == params.getKind()).count() == 0) {
        // TODO could partition files by executor sizes, however would need to do this in optimal
        // way.. not as easy as chop because need to result in a single file
        group = findMaximalRequiredSetToCompact(params.getCandidates(), maxFilesToCompact);
      }

      if (group.isEmpty() && params.getKind() == CompactionKind.CHOP) {
        // TODO since chop compactions do not have to result in a single file, could partition files
        // by executors sizes
        group = findMaximalRequiredSetToCompact(params.getCandidates(), maxFilesToCompact);
      }

      if (group.isEmpty()) {
        return new CompactionPlan();
      } else {
        // determine which executor to use based on the size of the files
        var ceid = getExecutor(group);

        if (!params.getRunningCompactions().isEmpty()) {
          // TODO remove
          log.info("Planning concurrent {} {}", ceid, group);
        }

        // TODO include type in priority!
        CompactionJob job =
            new CompactionJob(params.getAll().size(), ceid, group, params.getKind());
        return new CompactionPlan(List.of(job));
      }

    } catch (RuntimeException e) {
      // TODO remove
      log.warn("params:{}", params, e);
      throw e;
    }
  }

  private long getMaxSizeToCompact(CompactionKind kind) {
    if (kind == CompactionKind.SYSTEM) {
      Long max = executors.get(executors.size() - 1).maxSize;
      if (max == null)
        max = Long.MAX_VALUE;
    }
    return Long.MAX_VALUE;
  }

  /**
   * @return the expected files sizes for sets of compacting files.
   */
  private Set<CompactableFile> getExpected(Collection<CompactionJob> compacting) {

    Set<CompactableFile> expected = new HashSet<>();

    int count = 0;

    for (CompactionJob job : compacting) {
      count++;
      long size = job.getFiles().stream().mapToLong(CompactableFile::getEstimatedSize).sum();
      try {
        expected.add(CompactableFile.create(
            new URI("hdfs://fake/accumulo/tables/adef/t-zzFAKEzz/FAKE-0000" + count + ".rf"), size,
            0));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }

    }

    return expected;
  }

  public static Collection<CompactableFile>
      findMaximalRequiredSetToCompact(Collection<CompactableFile> files, int maxFilesToCompact) {

    if (files.size() <= maxFilesToCompact)
      return files;

    // TODO could reuse sorted files
    List<CompactableFile> sortedFiles = sortByFileSize(files);

    int numToCompact = maxFilesToCompact;

    if (sortedFiles.size() > maxFilesToCompact && sortedFiles.size() < 2 * maxFilesToCompact) {
      // on the second to last compaction pass, compact the minimum amount of files possible
      numToCompact = sortedFiles.size() - maxFilesToCompact + 1;
    }

    return sortedFiles.subList(0, numToCompact);
  }

  /**
   * Find the largest set of small files to compact.
   */
  public static Collection<CompactableFile> findMapFilesToCompact(Set<CompactableFile> files,
      double ratio, int maxFilesToCompact, long maxSizeToCompact) {
    if (files.size() <= 1)
      return Collections.emptySet();

    // sort files from smallest to largest. So position 0 has the smallest file.
    List<CompactableFile> sortedFiles = sortByFileSize(files);

    int larsmaIndex = -1;
    long larsmaSum = Long.MIN_VALUE;

    // index into sortedFiles, everything at and below this index meets the compaction ratio
    int goodIndex = -1;

    long sum = sortedFiles.get(0).getEstimatedSize();

    for (int c = 1; c < sortedFiles.size(); c++) {
      long currSize = sortedFiles.get(c).getEstimatedSize();
      sum += currSize;

      if (sum > maxSizeToCompact)
        break;

      if (currSize * ratio < sum) {
        goodIndex = c;

        if (goodIndex + 1 >= maxFilesToCompact)
          break; // TODO old algorithm used to slide a window up when nothing found in smallest
                 // files
      } else if (c - 1 == goodIndex) {
        // The previous file met the compaction ratio, but the current file does not. So all of the
        // previous files are candidates. However we must ensure that any candidate set produces a
        // file smaller than the next largest file in the next candidate set.
        if (larsmaIndex == -1 || larsmaSum > sortedFiles.get(goodIndex).getEstimatedSize()) {
          larsmaIndex = goodIndex;
          larsmaSum = sum - currSize;
        } else {
          break;
        }
      }
    }

    if (sortedFiles.size() - 1 == goodIndex
        && (larsmaIndex == -1 || larsmaSum > sortedFiles.get(goodIndex).getEstimatedSize())) {
      larsmaIndex = goodIndex;
    }

    if (larsmaIndex == -1)
      return Collections.emptySet();

    return sortedFiles.subList(0, larsmaIndex + 1);
  }

  CompactionExecutorId getExecutor(Collection<CompactableFile> files) {

    long size = files.stream().mapToLong(CompactableFile::getEstimatedSize).sum();

    for (Executor executor : executors) {
      if (size < executor.maxSize)
        return executor.ceid;
    }

    // TODO is this best behavior? Could not compact when there is no executor to service that size
    return executors.get(executors.size() - 1).ceid;
  }

  public static List<CompactableFile> sortByFileSize(Collection<CompactableFile> files) {
    List<CompactableFile> sortedFiles = new ArrayList<>(files);

    // sort from smallest file to largest
    Collections.sort(sortedFiles, Comparator.comparingLong(CompactableFile::getEstimatedSize)
        .thenComparing(CompactableFile::getUri));

    return sortedFiles;
  }
}
