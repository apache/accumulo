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
 * <li>Note that the CompactionCoordinator and at least one Compactor for "Large" must be running.
 * <li>{@code compaction.service.<service>.opts.maxOpen} This determines the maximum number of files
 * that will be included in a single compaction.
 * <li>{@code compaction.service.<service>.opts.groups} This is a json array of group objects which
 * have the following fields:
 * <table>
 * <caption>Default Compaction Planner Queue options</caption>
 * <tr>
 * <th>Field Name</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>name</td>
 * <td>name or alias of the group (required)</td>
 * </tr>
 * <tr>
 * <td>maxSize</td>
 * <td>threshold sum of the input files (required for all but one of the configs)</td>
 * </tr>
 * </table>
 * <br>
 * This 'groups' object is used for defining compaction groups. The maxSize field determines the
 * maximum size of compaction that will run in a group. The maxSize field can have a suffix of K,M,G
 * for kilobytes, megabytes, or gigabytes and represents the sum of the input files for a given
 * compaction. One group can have no max size and it will run everything that is too large for the
 * other groups. If all groups have a max size, then system compactions will only run for
 * compactions smaller than the largest max size. User, chop, and selector compactions will always
 * run, even if there is no group for their size. These compactions will run on the group with the
 * largest max size. The following example value for this property will create three separate
 * compaction groups. "small" will run compactions of files whose file size sum is less than 100M,
 * "medium" will run compactions of files whose file size sum is less than 500M, and "large" will
 * run all other compactions on Compactors configured to run compactions for Large:
 *
 * <pre>
 * {@code
 * [
 *  {"name":"small", "maxSize":"100M"},
 *  {"name":"medium", "maxSize":"500M"},
 *  {"name": "large"}
 * ]}
 * </pre>
 * </ul>
 *
 * @since 3.1.0
 * @see org.apache.accumulo.core.spi.compaction
 */

public class DefaultCompactionPlanner implements CompactionPlanner {

  private final static Logger log = LoggerFactory.getLogger(DefaultCompactionPlanner.class);

  private static class GroupConfig {
    String name;
    String maxSize;
  }

  private static class CompactionGroup {
    final CompactionGroupId cgid;
    final Long maxSize;

    public CompactionGroup(CompactionGroupId cgid, Long maxSize) {
      Preconditions.checkArgument(maxSize == null || maxSize > 0, "Invalid value for maxSize");
      this.cgid = Objects.requireNonNull(cgid, "Compaction ID is null");
      this.maxSize = maxSize;
    }

    Long getMaxSize() {
      return maxSize;
    }

    @Override
    public String toString() {
      return "[cgid=" + cgid + ", maxSize=" + maxSize + "]";
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

  private List<CompactionGroup> groups;
  private int maxFilesToCompact;

  @SuppressFBWarnings(value = {"UWF_UNWRITTEN_FIELD", "NP_UNWRITTEN_FIELD"},
      justification = "Field is written by Gson")
  @Override
  public void init(InitParameters params) {
    List<CompactionGroup> tmpGroups = new ArrayList<>();
    String values;

    if (params.getOptions().containsKey("groups") && !params.getOptions().get("groups").isBlank()) {
      values = params.getOptions().get("groups");

      // Generate a list of fields from the desired object.
      final List<String> groupFields = Arrays.stream(GroupConfig.class.getDeclaredFields())
          .map(Field::getName).collect(Collectors.toList());

      for (JsonElement element : GSON.get().fromJson(values, JsonArray.class)) {
        validateConfig(element, groupFields, GroupConfig.class.getName());
        GroupConfig groupConfig = GSON.get().fromJson(element, GroupConfig.class);

        Long maxSize = groupConfig.maxSize == null ? null
            : ConfigurationTypeHelper.getFixedMemoryAsBytes(groupConfig.maxSize);

        CompactionGroupId cgid;
        String group = Objects.requireNonNull(groupConfig.name, "'name' must be specified");
        cgid = params.getGroupManager().getGroup(group);
        tmpGroups.add(new CompactionGroup(cgid, maxSize));
      }
    }

    if (tmpGroups.size() < 1) {
      throw new IllegalStateException("No defined compaction groups for this planner");
    }

    tmpGroups.sort(Comparator.comparing(CompactionGroup::getMaxSize,
        Comparator.nullsLast(Comparator.naturalOrder())));

    groups = List.copyOf(tmpGroups);

    if (groups.stream().filter(g -> g.getMaxSize() == null).count() > 1) {
      throw new IllegalArgumentException(
          "Can only have one group w/o a maxSize. " + params.getOptions().get("groups"));
    }

    // use the add method on the Set interface to check for duplicate maxSizes
    Set<Long> maxSizes = new HashSet<>();
    groups.forEach(g -> {
      if (!maxSizes.add(g.getMaxSize())) {
        throw new IllegalArgumentException(
            "Duplicate maxSize set in groups. " + params.getOptions().get("groups"));
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
    // set of files to compact is computed and contains one of these files, then it's optimal to
    // wait
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
    compactionJobs.forEach(
        jobFiles -> builder.addJob(createPriority(params, jobFiles), getGroup(jobFiles), jobFiles));
    return builder.build();
  }

  private static short createPriority(PlanningParameters params,
      Collection<CompactableFile> group) {
    return CompactionJobPrioritizer.createPriority(params.getKind(), params.getAll().size(),
        group.size());
  }

  private long getMaxSizeToCompact(CompactionKind kind) {
    if (kind == CompactionKind.SYSTEM) {
      Long max = groups.get(groups.size() - 1).maxSize;
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

  CompactionGroupId getGroup(Collection<CompactableFile> files) {

    long size = files.stream().mapToLong(CompactableFile::getEstimatedSize).sum();

    for (CompactionGroup group : groups) {
      if (group.maxSize == null || size < group.maxSize) {
        return group.cgid;
      }
    }

    return groups.get(groups.size() - 1).cgid;
  }

  private static List<CompactableFile> sortByFileSize(Collection<CompactableFile> files) {
    ArrayList<CompactableFile> sortedFiles = new ArrayList<>(files);

    // sort from smallest file to largest
    Collections.sort(sortedFiles, Comparator.comparingLong(CompactableFile::getEstimatedSize)
        .thenComparing(CompactableFile::getUri));

    return sortedFiles;
  }
}
