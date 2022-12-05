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
import org.apache.accumulo.core.util.compaction.CompactionJobPrioritizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Finds the largest continuous set of small files that meet the compaction ratio and do not prevent
 * future compactions.
 *
 * <p>
 * The following configuration options are supported. Replace {@code <service>} with the name of the
 * compaction service you are configuring.
 *
 * <ul>
 * <li>{@code tserver.compaction.major.service.<service>.opts.executors} This is a json array of
 * objects where each object has the fields:
 * <table>
 * <caption>Default Compaction Planner Executor options</caption>
 * <tr>
 * <th>Field Name</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>name</td>
 * <td>name or alias of the executor (required)</td>
 * </tr>
 * <tr>
 * <td>type</td>
 * <td>valid values 'internal' or 'external' (required)</td>
 * </tr>
 * <tr>
 * <td>maxSize</td>
 * <td>threshold sum of the input files (required for all but one of the configs)</td>
 * </tr>
 * <tr>
 * <td>numThreads</td>
 * <td>number of threads for this executor configuration (required for 'internal', cannot be
 * specified for 'external')</td>
 * </tr>
 * <tr>
 * <td>queue</td>
 * <td>name of the external compaction queue (required for 'external', cannot be specified for
 * 'internal')</td>
 * </tr>
 * </table>
 * <br>
 * The maxSize field determines the maximum size of compaction that will run on an executor. The
 * maxSize field can have a suffix of K,M,G for kilobytes, megabytes, or gigabytes and represents
 * the sum of the input files for a given compaction. One executor can have no max size and it will
 * run everything that is too large for the other executors. If all executors have a max size, then
 * system compactions will only run for compactions smaller than the largest max size. User, chop,
 * and selector compactions will always run, even if there is no executor for their size. These
 * compactions will run on the executor with the largest max size. The following example value for
 * this property will create 3 threads to run compactions of files whose file size sum is less than
 * 100M, 3 threads to run compactions of files whose file size sum is less than 500M, and run all
 * other compactions on Compactors configured to run compactions for Queue1:
 *
 * <pre>
 * {@code
 * [{"name":"small", "type": "internal", "maxSize":"100M","numThreads":3},
 *  {"name":"medium", "type": "internal", "maxSize":"500M","numThreads":3},
 *  {"name: "large", "type": "external", "queue", "Queue1"}
 * ]}
 * </pre>
 *
 * Note that the use of 'external' requires that the CompactionCoordinator and at least one
 * Compactor for Queue1 is running.
 * <li>{@code tserver.compaction.major.service.<service>.opts.maxOpen} This determines the maximum
 * number of files that will be included in a single compaction.
 * </ul>
 *
 * @since 2.1.0
 * @see org.apache.accumulo.core.spi.compaction
 */

public class DefaultCompactionPlanner implements CompactionPlanner {

  private static final Logger log = LoggerFactory.getLogger(DefaultCompactionPlanner.class);

  public static class ExecutorConfig {
    String type;
    String name;
    String maxSize;
    Integer numThreads;
    String queue;
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

    @Override
    public String toString() {
      return "[ceid=" + ceid + ", maxSize=" + maxSize + "]";
    }
  }

  private List<Executor> executors;
  private int maxFilesToCompact;

  @SuppressFBWarnings(value = {"UWF_UNWRITTEN_FIELD", "NP_UNWRITTEN_FIELD"},
      justification = "Field is written by Gson")
  @Override
  public void init(InitParameters params) {
    ExecutorConfig[] execConfigs =
        new Gson().fromJson(params.getOptions().get("executors"), ExecutorConfig[].class);

    List<Executor> tmpExec = new ArrayList<>();

    for (ExecutorConfig executorConfig : execConfigs) {
      Long maxSize = executorConfig.maxSize == null ? null
          : ConfigurationTypeHelper.getFixedMemoryAsBytes(executorConfig.maxSize);

      CompactionExecutorId ceid;

      // If not supplied, GSON will leave type null. Default to internal
      if (executorConfig.type == null) {
        executorConfig.type = "internal";
      }

      switch (executorConfig.type) {
        case "internal":
          Preconditions.checkArgument(null == executorConfig.queue,
              "'queue' should not be specified for internal compactions");
          int numThreads = Objects.requireNonNull(executorConfig.numThreads,
              "'numThreads' must be specified for internal type");
          ceid = params.getExecutorManager().createExecutor(executorConfig.name, numThreads);
          break;
        case "external":
          Preconditions.checkArgument(null == executorConfig.numThreads,
              "'numThreads' should not be specified for external compactions");
          String queue = Objects.requireNonNull(executorConfig.queue,
              "'queue' must be specified for external type");
          ceid = params.getExecutorManager().getExternalExecutor(queue);
          break;
        default:
          throw new IllegalArgumentException("type must be 'internal' or 'external'");
      }
      tmpExec.add(new Executor(ceid, maxSize));
    }

    Collections.sort(tmpExec, Comparator.comparing(Executor::getMaxSize,
        Comparator.nullsLast(Comparator.naturalOrder())));

    executors = List.copyOf(tmpExec);

    if (executors.stream().filter(e -> e.getMaxSize() == null).count() > 1) {
      throw new IllegalArgumentException(
          "Can only have one executor w/o a maxSize. " + params.getOptions().get("executors"));
    }

    // use the add method on the Set interface to check for duplicate maxSizes
    Set<Long> maxSizes = new HashSet<>();
    executors.forEach(e -> {
      if (!maxSizes.add(e.getMaxSize())) {
        throw new IllegalArgumentException(
            "Duplicate maxSize set in executors. " + params.getOptions().get("executors"));
      }
    });

    determineMaxFilesToCompact(params);
  }

  @SuppressWarnings("removal")
  private void determineMaxFilesToCompact(InitParameters params) {
    String fqo = params.getFullyQualifiedOption("maxOpen");
    if (!params.getServiceEnvironment().getConfiguration().isSet(fqo)
        && params.getServiceEnvironment().getConfiguration()
            .isSet(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey())) {
      log.warn("The property " + Property.TSERV_MAJC_THREAD_MAXOPEN.getKey()
          + " was set, it is deprecated.  Set the " + fqo + " option instead.");
      this.maxFilesToCompact = Integer.parseInt(params.getServiceEnvironment().getConfiguration()
          .get(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey()));
    } else {
      this.maxFilesToCompact = Integer.parseInt(params.getOptions().getOrDefault("maxOpen", "10"));
    }
  }

  @Override
  public CompactionPlan makePlan(PlanningParameters params) {
    try {

      if (params.getCandidates().isEmpty()) {
        return params.createPlanBuilder().build();
      }

      Set<CompactableFile> filesCopy = new HashSet<>(params.getCandidates());

      long maxSizeToCompact = getMaxSizeToCompact(params.getKind());

      Collection<CompactableFile> group;
      if (params.getRunningCompactions().isEmpty()) {
        group = findMapFilesToCompact(filesCopy, params.getRatio(), maxFilesToCompact,
            maxSizeToCompact);

        if (!group.isEmpty() && group.size() < params.getCandidates().size()
            && params.getCandidates().size() <= maxFilesToCompact
            && (params.getKind() == CompactionKind.USER
                || params.getKind() == CompactionKind.SELECTOR)) {
          // USER and SELECTOR compactions must eventually compact all files. When a subset of files
          // that meets the compaction ratio is selected, look ahead and see if the next compaction
          // would also meet the compaction ratio. If not then compact everything to avoid doing
          // more than logarithmic work across multiple comapctions.

          filesCopy.removeAll(group);
          filesCopy.add(getExpected(group, 0));

          if (findMapFilesToCompact(filesCopy, params.getRatio(), maxFilesToCompact,
              maxSizeToCompact).isEmpty()) {
            // The next possible compaction does not meet the compaction ratio, so compact
            // everything.
            group = Set.copyOf(params.getCandidates());
          }

        }

      } else if (params.getKind() == CompactionKind.SYSTEM) {
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
      } else {
        group = Set.of();
      }

      if (group.isEmpty()
          && (params.getKind() == CompactionKind.USER || params.getKind() == CompactionKind.SELECTOR
              || params.getKind() == CompactionKind.CHOP)
          && params.getRunningCompactions().stream()
              .noneMatch(job -> job.getKind() == params.getKind())) {
        group = findMaximalRequiredSetToCompact(params.getCandidates(), maxFilesToCompact);
      }

      if (group.isEmpty()) {
        return params.createPlanBuilder().build();
      } else {
        // determine which executor to use based on the size of the files
        var ceid = getExecutor(group);

        return params.createPlanBuilder().addJob(createPriority(params, group), ceid, group)
            .build();
      }
    } catch (RuntimeException e) {
      throw e;
    }
  }

  private static short createPriority(PlanningParameters params,
      Collection<CompactableFile> group) {
    return CompactionJobPrioritizer.createPriority(params.getKind(), params.getAll().size(),
        group.size());
  }

  private long getMaxSizeToCompact(CompactionKind kind) {
    if (kind == CompactionKind.SYSTEM) {
      Long max = executors.get(executors.size() - 1).maxSize;
      if (max != null) {
        return max;
      }
    }
    return Long.MAX_VALUE;
  }

  private CompactableFile getExpected(Collection<CompactableFile> files, int count) {
    long size = files.stream().mapToLong(CompactableFile::getEstimatedSize).sum();
    try {
      return CompactableFile.create(
          new URI("hdfs://fake/accumulo/tables/adef/t-zzFAKEzz/FAKE-0000" + count + ".rf"), size,
          0);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return the expected files sizes for sets of compacting files.
   */
  private Set<CompactableFile> getExpected(Collection<CompactionJob> compacting) {

    Set<CompactableFile> expected = new HashSet<>();

    int count = 0;

    for (CompactionJob job : compacting) {
      count++;
      expected.add(getExpected(job.getFiles(), count));
    }

    return expected;
  }

  public static Collection<CompactableFile>
      findMaximalRequiredSetToCompact(Collection<CompactableFile> files, int maxFilesToCompact) {

    if (files.size() <= maxFilesToCompact) {
      return files;
    }

    List<CompactableFile> sortedFiles = sortByFileSize(files);

    int numToCompact = maxFilesToCompact;

    if (sortedFiles.size() > maxFilesToCompact && sortedFiles.size() < 2 * maxFilesToCompact) {
      // on the second to last compaction pass, compact the minimum amount of files possible
      numToCompact = sortedFiles.size() - maxFilesToCompact + 1;
    }

    return sortedFiles.subList(0, numToCompact);
  }

  public static Collection<CompactableFile> findMapFilesToCompact(Set<CompactableFile> files,
      double ratio, int maxFilesToCompact, long maxSizeToCompact) {
    if (files.size() <= 1) {
      return Collections.emptySet();
    }

    // sort files from smallest to largest. So position 0 has the smallest file.
    List<CompactableFile> sortedFiles = sortByFileSize(files);

    int maxSizeIndex = sortedFiles.size();
    long sum = 0;
    for (int i = 0; i < sortedFiles.size(); i++) {
      sum += sortedFiles.get(i).getEstimatedSize();
      if (sum > maxSizeToCompact) {
        maxSizeIndex = i;
        break;
      }
    }

    if (maxSizeIndex < sortedFiles.size()) {
      sortedFiles = sortedFiles.subList(0, maxSizeIndex);
      if (sortedFiles.size() <= 1) {
        return Collections.emptySet();
      }
    }

    var loops = Math.max(1, sortedFiles.size() - maxFilesToCompact + 1);
    for (int i = 0; i < loops; i++) {
      var filesToCompact = findMapFilesToCompact(
          sortedFiles.subList(i, Math.min(sortedFiles.size(), maxFilesToCompact) + i), ratio);
      if (!filesToCompact.isEmpty()) {
        return filesToCompact;
      }
    }

    return Collections.emptySet();

  }

  /**
   * Find the largest set of contiguous small files that meet the compaction ratio. For a set of
   * file size like [101M,102M,103M,104M,4M,3M,3M,3M,3M], it would be nice compact the smaller files
   * [4M,3M,3M,3M,3M] followed by the larger ones. The reason to do the smaller ones first is to
   * more quickly reduce the number of files. However, all compactions should still follow the
   * compaction ratio in order to ensure the amount of data rewriting is logarithmic.
   *
   * <p>
   * A set of files meets the compaction ratio when the largestFileinSet * compactionRatio &lt;
   * sumOfFileSizesInSet. This algorithm grows the set of small files until it meets the compaction
   * ratio, then keeps growing it while it continues to meet the ratio. Once a set does not meet the
   * compaction ratio, the last set that did is returned. Growing the set of small files means
   * adding the smallest file not in the set.
   *
   * <p>
   * There is one caveat to the algorithm mentioned above, if a smaller set of files would prevent a
   * future compaction then do not select it. This code in this function performs a look ahead to
   * see if a candidate set will prevent future compactions.
   *
   * <p>
   * As an example of a small set of files that could prevent a future compaction, consider the
   * files sizes [100M,99M,33M,33M,33M,33M]. For a compaction ratio of 3, the set
   * [100M,99M,33M,33M,33M,33M] and [33M,33M,33M,33M] both meet the compaction ratio. If the set
   * [33M,33M,33M,33M] is compacted, then it will result in a tablet having [132M, 100M, 99M] which
   * does not meet the compaction ration. So in this case, choosing the set [33M,33M,33M,33M]
   * prevents a future compaction that could have occurred. This function will not choose the
   * smaller set because of it would prevent the future compaction.
   */
  private static Collection<CompactableFile>
      findMapFilesToCompact(List<CompactableFile> sortedFiles, double ratio) {

    int larsmaIndex = -1;
    long larsmaSum = Long.MIN_VALUE;

    // index into sortedFiles, everything at and below this index meets the compaction ratio
    int goodIndex = -1;

    long sum = sortedFiles.get(0).getEstimatedSize();

    for (int c = 1; c < sortedFiles.size(); c++) {
      long currSize = sortedFiles.get(c).getEstimatedSize();

      // ensure data is sorted
      Preconditions.checkArgument(currSize >= sortedFiles.get(c - 1).getEstimatedSize());

      sum += currSize;

      if (currSize * ratio < sum) {
        goodIndex = c;
      } else if (c - 1 == goodIndex) {
        // The previous file met the compaction ratio, but the current file does not. So all of the
        // previous files are candidates. However we must ensure that any candidate set produces a
        // file smaller than the next largest file in the next candidate set to ensure future
        // compactions are not prevented.
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

    if (larsmaIndex == -1) {
      return Collections.emptySet();
    }

    return sortedFiles.subList(0, larsmaIndex + 1);
  }

  CompactionExecutorId getExecutor(Collection<CompactableFile> files) {

    long size = files.stream().mapToLong(CompactableFile::getEstimatedSize).sum();

    for (Executor executor : executors) {
      if (executor.maxSize == null || size < executor.maxSize) {
        return executor.ceid;
      }
    }

    return executors.get(executors.size() - 1).ceid;
  }

  public static List<CompactableFile> sortByFileSize(Collection<CompactableFile> files) {
    ArrayList<CompactableFile> sortedFiles = new ArrayList<>(files);

    // sort from smallest file to largest
    Collections.sort(sortedFiles, Comparator.comparingLong(CompactableFile::getEstimatedSize)
        .thenComparing(CompactableFile::getUri));

    return sortedFiles;
  }
}
