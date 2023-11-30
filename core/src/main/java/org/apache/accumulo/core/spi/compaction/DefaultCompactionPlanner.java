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

import static org.apache.accumulo.core.util.LazySingletons.GSON;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.util.compaction.CompactionJobPrioritizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

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
 * <li>{@code compaction.service.<service>.opts.executors} This is a json array of objects where
 * each object has the fields:
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
 * <td>group</td>
 * <td>name of the external compaction group (required for 'external', cannot be specified for
 * 'internal')</td>
 * </tr>
 * </table>
 * <br>
 * Note: The "executors" option has been deprecated in 3.1 and will be removed in a future release.
 * This example uses the new `compaction.service` prefix. The property prefix
 * "tserver.compaction.major.service" has also been deprecated in 3.1 and will be removed in a
 * future release. The maxSize field determines the maximum size of compaction that will run on an
 * executor. The maxSize field can have a suffix of K,M,G for kilobytes, megabytes, or gigabytes and
 * represents the sum of the input files for a given compaction. One executor can have no max size
 * and it will run everything that is too large for the other executors. If all executors have a max
 * size, then system compactions will only run for compactions smaller than the largest max size.
 * User, chop, and selector compactions will always run, even if there is no executor for their
 * size. These compactions will run on the executor with the largest max size. The following example
 * value for this property will create 3 threads to run compactions of files whose file size sum is
 * less than 100M, 3 threads to run compactions of files whose file size sum is less than 500M, and
 * run all other compactions on Compactors configured to run compactions for Queue1:
 *
 * <pre>
 * {@code
 * [
 *  {"name":"small", "type": "internal", "maxSize":"100M","numThreads":3},
 *  {"name":"medium", "type": "internal", "maxSize":"500M","numThreads":3},
 *  {"name": "large", "type": "external", "group", "Queue1"}
 * ]}
 * </pre>
 *
 * Note that the use of 'external' requires that the CompactionCoordinator and at least one
 * Compactor for Queue1 is running.
 * <li>{@code compaction.service.<service>.opts.maxOpen} This determines the maximum number of files
 * that will be included in a single compaction.
 * <li>{@code compaction.service.<service>.opts.queues} This is a json array of queue objects which
 * have the following fields:
 * <table>
 * <caption>Default Compaction Planner Queue options</caption>
 * <tr>
 * <th>Field Name</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>name</td>
 * <td>name or alias of the queue (required)</td>
 * </tr>
 * <tr>
 * <td>maxSize</td>
 * <td>threshold sum of the input files (required for all but one of the configs)</td>
 * </tr>
 * </table>
 * <br>
 * This 'queues' object is used for defining external compaction queues without needing to use the
 * thread-based 'executors' property.
 * </ul>
 *
 * @since 3.1.0
 * @see org.apache.accumulo.core.spi.compaction
 */

public class DefaultCompactionPlanner implements CompactionPlanner {

  private final static Logger log = LoggerFactory.getLogger(DefaultCompactionPlanner.class);

  private static class ExecutorConfig {
    String type;
    String name;
    String maxSize;
    Integer numThreads;
    String group;

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getMaxSize() {
      return maxSize;
    }

    public void setMaxSize(String maxSize) {
      this.maxSize = maxSize;
    }

    public Integer getNumThreads() {
      return numThreads;
    }

    public void setNumThreads(Integer numThreads) {
      this.numThreads = numThreads;
    }

    public String getQueue() {
      return group;
    }

    public void setGroup(String group) {
      this.group = group;
    }

  }

  private static class QueueConfig {
    String name;
    String maxSize;
  }

  private static class Executor {
    final CompactionExecutorId ceid;
    final Long maxSize;

    public Executor(CompactionExecutorId ceid, Long maxSize) {
      Preconditions.checkArgument(maxSize == null || maxSize > 0, "Invalid value for maxSize");
      this.ceid = Objects.requireNonNull(ceid, "Compaction ID is null");
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

  private static class FakeFileGenerator {

    private int count = 0;

    public CompactableFile create(long size) {
      try {
        count++;
        return CompactableFile.create(
            new URI("hdfs://fake/accumulo/tables/adef/t-zzFAKEzz/FAKE-0000" + count + ".rf"), size,
            0);
      } catch (URISyntaxException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private List<Executor> executors;
  private int maxFilesToCompact;

  @SuppressFBWarnings(value = {"UWF_UNWRITTEN_FIELD", "NP_UNWRITTEN_FIELD"},
      justification = "Field is written by Gson")
  @Override
  public void init(InitParameters params) {
    List<Executor> tmpExec = new ArrayList<>();
    String values;

    if (params.getOptions().containsKey("executors")
        && !params.getOptions().get("executors").isBlank()) {
      values = params.getOptions().get("executors");

      // Generate a list of fields from the desired object.
      final List<String> execFields = Arrays.stream(ExecutorConfig.class.getDeclaredFields())
          .map(Field::getName).collect(Collectors.toList());

      for (JsonElement element : GSON.get().fromJson(values, JsonArray.class)) {
        validateConfig(element, execFields, ExecutorConfig.class.getName());
        ExecutorConfig executorConfig = GSON.get().fromJson(element, ExecutorConfig.class);

        Long maxSize = executorConfig.maxSize == null ? null
            : ConfigurationTypeHelper.getFixedMemoryAsBytes(executorConfig.maxSize);
        CompactionExecutorId ceid;

        // If not supplied, GSON will leave type null. Default to internal
        if (executorConfig.type == null) {
          executorConfig.type = "internal";
        }

        switch (executorConfig.type) {
          case "internal":
            Preconditions.checkArgument(null == executorConfig.group,
                "'group' should not be specified for internal compactions");
            int numThreads = Objects.requireNonNull(executorConfig.numThreads,
                "'numThreads' must be specified for internal type");
            ceid = params.getExecutorManager().createExecutor(executorConfig.name, numThreads);
            break;
          case "external":
            Preconditions.checkArgument(null == executorConfig.numThreads,
                "'numThreads' should not be specified for external compactions");
            String group = Objects.requireNonNull(executorConfig.group,
                "'group' must be specified for external type");
            ceid = params.getExecutorManager().getExternalExecutor(group);
            break;
          default:
            throw new IllegalArgumentException("type must be 'internal' or 'external'");
        }
        tmpExec.add(new Executor(ceid, maxSize));
      }
    }

    if (params.getOptions().containsKey("queues") && !params.getOptions().get("queues").isBlank()) {
      values = params.getOptions().get("queues");

      // Generate a list of fields from the desired object.
      final List<String> queueFields = Arrays.stream(QueueConfig.class.getDeclaredFields())
          .map(Field::getName).collect(Collectors.toList());

      for (JsonElement element : GSON.get().fromJson(values, JsonArray.class)) {
        validateConfig(element, queueFields, QueueConfig.class.getName());
        QueueConfig queueConfig = GSON.get().fromJson(element, QueueConfig.class);

        Long maxSize = queueConfig.maxSize == null ? null
            : ConfigurationTypeHelper.getFixedMemoryAsBytes(queueConfig.maxSize);

        CompactionExecutorId ceid;
        String queue = Objects.requireNonNull(queueConfig.name, "'name' must be specified");
        ceid = params.getExecutorManager().getExternalExecutor(queue);
        tmpExec.add(new Executor(ceid, maxSize));
      }
    }

    if (tmpExec.size() < 1) {
      throw new IllegalStateException("No defined executors or queues for this planner");
    }

    tmpExec.sort(Comparator.comparing(Executor::getMaxSize,
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

  private void determineMaxFilesToCompact(InitParameters params) {

    String maxOpen = params.getOptions().get("maxOpen");
    if (maxOpen == null) {
      maxOpen = "10";
      log.trace("default maxOpen not set, defaulting to 10");
    }
    this.maxFilesToCompact = Integer.parseInt(maxOpen);
  }

  private void validateConfig(JsonElement json, List<String> fields, String className) {

    JsonObject jsonObject = GSON.get().fromJson(json, JsonObject.class);

    List<String> objectProperties = new ArrayList<>(jsonObject.keySet());
    HashSet<String> classFieldNames = new HashSet<>(fields);

    if (!classFieldNames.containsAll(objectProperties)) {
      objectProperties.removeAll(classFieldNames);
      throw new JsonParseException(
          "Invalid fields: " + objectProperties + " provided for class: " + className);
    }
  }

  @Override
  public CompactionPlan makePlan(PlanningParameters params) {
    if (params.getCandidates().isEmpty()) {
      return params.createPlanBuilder().build();
    }

    Set<CompactableFile> filesCopy = new HashSet<>(params.getCandidates());

    FakeFileGenerator fakeFileGenerator = new FakeFileGenerator();

    long maxSizeToCompact = getMaxSizeToCompact(params.getKind());

    // This set represents future files that will be produced by running compactions. If the optimal
    // set of files to compact is computed and contains one of these files, then its optimal to wait
    // for this compaction to finish.
    Set<CompactableFile> expectedFiles = new HashSet<>();
    params.getRunningCompactions().stream().filter(job -> job.getKind() == params.getKind())
        .map(job -> getExpected(job.getFiles(), fakeFileGenerator))
        .forEach(compactableFile -> Preconditions.checkState(expectedFiles.add(compactableFile)));
    Preconditions.checkState(Collections.disjoint(expectedFiles, filesCopy));
    filesCopy.addAll(expectedFiles);

    List<Collection<CompactableFile>> compactionJobs = new ArrayList<>();

    while (true) {
      var filesToCompact =
          findDataFilesToCompact(filesCopy, params.getRatio(), maxFilesToCompact, maxSizeToCompact);
      if (!Collections.disjoint(filesToCompact, expectedFiles)) {
        // the optimal set of files to compact includes the output of a running compaction, so lets
        // wait for that running compaction to finish.
        break;
      }

      if (filesToCompact.isEmpty()) {
        break;
      }

      filesCopy.removeAll(filesToCompact);

      // A compaction job will be created for these files, so lets add an expected file for that
      // planned compaction job. Then if future iterations of this loop will include that file then
      // they will not compact.
      var expectedFile = getExpected(filesToCompact, fakeFileGenerator);
      Preconditions.checkState(expectedFiles.add(expectedFile));
      Preconditions.checkState(filesCopy.add(expectedFile));

      compactionJobs.add(filesToCompact);

      if (filesToCompact.size() < maxFilesToCompact) {
        // Only continue looking for more compaction jobs when a set of files is found equals
        // maxFilesToCompact in size. When the files found is less than the max size its an
        // indication that the compaction ratio was no longer met and therefore it would be
        // suboptimal to look for more jobs because the smallest optimal set was found.
        break;
      }
    }

    if (compactionJobs.size() == 1
        && (params.getKind() == CompactionKind.USER || params.getKind() == CompactionKind.SELECTOR)
        && compactionJobs.get(0).size() < params.getCandidates().size()
        && compactionJobs.get(0).size() <= maxFilesToCompact) {
      // USER and SELECTOR compactions must eventually compact all files. When a subset of files
      // that meets the compaction ratio is selected, look ahead and see if the next compaction
      // would also meet the compaction ratio. If not then compact everything to avoid doing
      // more than logarithmic work across multiple comapctions.

      var group = compactionJobs.get(0);
      var candidatesCopy = new HashSet<>(params.getCandidates());

      candidatesCopy.removeAll(group);
      Preconditions.checkState(candidatesCopy.add(getExpected(group, fakeFileGenerator)));

      if (findDataFilesToCompact(candidatesCopy, params.getRatio(), maxFilesToCompact,
          maxSizeToCompact).isEmpty()) {
        // The next possible compaction does not meet the compaction ratio, so compact
        // everything.
        compactionJobs.set(0, Set.copyOf(params.getCandidates()));
      }
    }

    if (compactionJobs.isEmpty()
        && (params.getKind() == CompactionKind.USER || params.getKind() == CompactionKind.SELECTOR)
        && params.getRunningCompactions().stream()
            .noneMatch(job -> job.getKind() == params.getKind())) {
      // These kinds of compaction require files to compact even if none of the files meet the
      // compaction ratio. No files were found using the compaction ratio and no compactions are
      // running, so force a compaction.
      compactionJobs = findMaximalRequiredSetToCompact(params.getCandidates(), maxFilesToCompact);
    }

    var builder = params.createPlanBuilder();
    compactionJobs.forEach(jobFiles -> builder.addJob(createPriority(params, jobFiles),
        getExecutor(jobFiles), jobFiles));
    return builder.build();
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

  private CompactableFile getExpected(Collection<CompactableFile> files,
      FakeFileGenerator fakeFileGenerator) {
    long size = files.stream().mapToLong(CompactableFile::getEstimatedSize).sum();
    return fakeFileGenerator.create(size);
  }

  private static List<Collection<CompactableFile>>
      findMaximalRequiredSetToCompact(Collection<CompactableFile> files, int maxFilesToCompact) {

    if (files.size() <= maxFilesToCompact) {
      return List.of(files);
    }

    List<CompactableFile> sortedFiles = sortByFileSize(files);

    // compute the number of full compaction jobs with full files that could run and then subtract
    // 1. The 1 is subtracted because the last job is a special case.
    int batches = sortedFiles.size() / maxFilesToCompact - 1;

    if (batches > 0) {
      ArrayList<Collection<CompactableFile>> jobs = new ArrayList<>();
      for (int i = 0; i < batches; i++) {
        jobs.add(sortedFiles.subList(i * maxFilesToCompact, (i + 1) * maxFilesToCompact));
      }
      return jobs;
    } else {
      int numToCompact = maxFilesToCompact;

      if (sortedFiles.size() > maxFilesToCompact && sortedFiles.size() < 2 * maxFilesToCompact) {
        // On the second to last compaction pass, compact the minimum amount of files possible. This
        // is done to avoid unnecessarily compacting the largest files more than once.
        numToCompact = sortedFiles.size() - maxFilesToCompact + 1;
      }

      return List.of(sortedFiles.subList(0, numToCompact));
    }
  }

  static Collection<CompactableFile> findDataFilesToCompact(Set<CompactableFile> files,
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
      var filesToCompact = findDataFilesToCompact(
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
      findDataFilesToCompact(List<CompactableFile> sortedFiles, double ratio) {

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

  private static List<CompactableFile> sortByFileSize(Collection<CompactableFile> files) {
    ArrayList<CompactableFile> sortedFiles = new ArrayList<>(files);

    // sort from smallest file to largest
    Collections.sort(sortedFiles, Comparator.comparingLong(CompactableFile::getEstimatedSize)
        .thenComparing(CompactableFile::getUri));

    return sortedFiles;
  }
}
