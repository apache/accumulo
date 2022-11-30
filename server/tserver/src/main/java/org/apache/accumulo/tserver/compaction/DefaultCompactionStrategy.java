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
package org.apache.accumulo.tserver.compaction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

@SuppressWarnings("removal")
public class DefaultCompactionStrategy extends CompactionStrategy {

  /**
   * Keeps track of the sum of the size of all files within a window. The files are sorted from
   * largest to smallest. Supports efficiently creating sub windows, sliding the window, and
   * shrinking the window.
   */
  @VisibleForTesting
  static class SizeWindow {

    List<CompactionFile> files;
    long sum = 0;

    int first;
    int last;

    SizeWindow() {}

    SizeWindow(Map<StoredTabletFile,DataFileValue> allFiles) {
      files = new ArrayList<>();
      for (Entry<StoredTabletFile,DataFileValue> entry : allFiles.entrySet()) {
        files.add(new CompactionFile(entry.getKey(), entry.getValue().getSize()));
      }

      files.sort(Comparator.comparingLong(CompactionFile::getSize)
          .thenComparing(CompactionFile::getFile).reversed());

      for (CompactionFile file : files) {
        sum += file.size;
      }

      first = 0;
      last = files.size();
    }

    void pop() {
      if (first >= last) {
        throw new IllegalStateException("Can not pop");
      }

      sum -= files.get(first).size;
      first++;
    }

    long topSize() {
      return files.get(first).size;
    }

    boolean slideUp() {
      if (first == 0) {
        return false;
      }

      first--;
      last--;

      sum += files.get(first).size;
      sum -= files.get(last).size;

      return true;
    }

    SizeWindow tail(int windowSize) {
      Preconditions.checkArgument(windowSize > 0);

      SizeWindow sub = new SizeWindow();

      sub.files = files;
      sub.first = Math.max(last - windowSize, first);
      sub.last = last;
      sub.sum = 0;

      for (int i = sub.first; i < sub.last; i++) {
        sub.sum += files.get(i).size;
      }

      return sub;
    }

    long sum() {
      return sum;
    }

    int size() {
      return (last - first);
    }

    public List<StoredTabletFile> getFiles() {
      List<StoredTabletFile> windowFiles = new ArrayList<>(size());
      for (int i = first; i < last; i++) {
        windowFiles.add(files.get(i).file);
      }
      return windowFiles;
    }

    @Override
    public String toString() {
      return "size:" + size() + " sum:" + sum() + " first:" + first + " last:" + last + " topSize:"
          + topSize();
    }
  }

  @Override
  public boolean shouldCompact(MajorCompactionRequest request) {
    CompactionPlan plan = getCompactionPlan(request);
    return plan != null && !plan.inputFiles.isEmpty();
  }

  @Override
  public CompactionPlan getCompactionPlan(MajorCompactionRequest request) {
    CompactionPlan result = new CompactionPlan();

    List<StoredTabletFile> toCompact = findMapFilesToCompact(request);
    if (toCompact == null || toCompact.isEmpty()) {
      return result;
    }
    result.inputFiles.addAll(toCompact);
    return result;
  }

  private static class CompactionFile {
    public StoredTabletFile file;
    public long size;

    public CompactionFile(StoredTabletFile file, long size) {
      this.file = file;
      this.size = size;
    }

    long getSize() {
      return size;
    }

    TabletFile getFile() {
      return file;
    }
  }

  private List<StoredTabletFile> findMapFilesToCompact(MajorCompactionRequest request) {
    MajorCompactionReason reason = request.getReason();
    if (reason == MajorCompactionReason.USER) {
      return new ArrayList<>(request.getFiles().keySet());
    }

    if (reason == MajorCompactionReason.CHOP) {
      // should not happen, but this is safe
      return new ArrayList<>(request.getFiles().keySet());
    }

    if (request.getFiles().size() <= 1) {
      return null;
    }

    double ratio = Double.parseDouble(request.getTableConfig(Property.TABLE_MAJC_RATIO.getKey()));
    int maxFilesToCompact =
        Integer.parseInt(request.getTableConfig(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey()));
    int maxFilesPerTablet = request.getMaxFilesPerTablet();

    int minFilesToCompact = 0;
    if (request.getFiles().size() > maxFilesPerTablet) {
      minFilesToCompact = request.getFiles().size() - maxFilesPerTablet + 1;
    }

    minFilesToCompact = Math.min(minFilesToCompact, maxFilesToCompact);

    SizeWindow all = new SizeWindow(request.getFiles());

    List<StoredTabletFile> files = null;

    // Within a window of size maxFilesToCompact containing the smallest files check to see if any
    // files meet the compaction ratio criteria.
    SizeWindow window = all.tail(maxFilesToCompact);
    while (window.size() > 1 && files == null) {

      if (window.topSize() * ratio <= window.sum()) {
        files = window.getFiles();
      }

      window.pop();
    }

    // Previous search was fruitless. If there are more files than maxFilesToCompact, then try
    // sliding the window up looking for files that meet the criteria.
    if (files == null || files.size() < minFilesToCompact) {
      window = all.tail(maxFilesToCompact);

      files = null;

      // When moving the window up there is no need to pop/shrink the window. All possible sets are
      // covered without doing this. Proof is left as an exercise for the reader. This is predicated
      // on the first search shrinking the initial window.
      while (window.slideUp() && files == null) {
        if (window.topSize() * ratio <= window.sum()) {
          files = window.getFiles();
        }
      }
    }

    // Ensure the minimum number of files are compacted.
    if ((files != null && files.size() < minFilesToCompact)
        || (files == null && minFilesToCompact > 0)) {
      // get the smallest files of size minFilesToCompact
      files = all.tail(minFilesToCompact).getFiles();
    }

    return files;
  }
}
