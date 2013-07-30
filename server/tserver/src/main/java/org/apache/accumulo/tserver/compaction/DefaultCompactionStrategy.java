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
package org.apache.accumulo.tserver.compaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.server.fs.FileRef;

public class DefaultCompactionStrategy extends CompactionStrategy {

  @Override
  public boolean shouldCompact(MajorCompactionRequest request) throws IOException {
    CompactionPlan plan = getCompactionPlan(request);
    return plan != null && !plan.inputFiles.isEmpty();
  }

  @Override
  public CompactionPlan getCompactionPlan(MajorCompactionRequest request) throws IOException {
    CompactionPlan result = new CompactionPlan();

    List<FileRef> toCompact = findMapFilesToCompact(request);
    if (toCompact == null || toCompact.isEmpty())
      return result;
    result.inputFiles.addAll(toCompact);
    return result;
  }

  private static class CompactionFile {
    public FileRef file;
    public long size;

    public CompactionFile(FileRef file, long size) {
      super();
      this.file = file;
      this.size = size;
    }
  }

  private List<FileRef> findMapFilesToCompact(MajorCompactionRequest request) {
    MajorCompactionReason reason = request.getReason();
    if (reason == MajorCompactionReason.USER) {
      return new ArrayList<FileRef>(request.getFiles().keySet());
    }
    if (reason == MajorCompactionReason.CHOP) {
      // should not happen, but this is safe
      return new ArrayList<FileRef>(request.getFiles().keySet());
    }

    if (request.getFiles().size() <= 1)
      return null;
    TreeSet<CompactionFile> candidateFiles = new TreeSet<CompactionFile>(new Comparator<CompactionFile>() {
      @Override
      public int compare(CompactionFile o1, CompactionFile o2) {
        if (o1 == o2)
          return 0;
        if (o1.size < o2.size)
          return -1;
        if (o1.size > o2.size)
          return 1;
        return o1.file.compareTo(o2.file);
      }
    });

    double ratio = Double.parseDouble(request.getTableConfig(Property.TABLE_MAJC_RATIO.getKey()));
    int maxFilesToCompact = Integer.parseInt(request.getTableConfig(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey()));
    int maxFilesPerTablet = request.getMaxFilesPerTablet();

    for (Entry<FileRef,DataFileValue> entry : request.getFiles().entrySet()) {
      candidateFiles.add(new CompactionFile(entry.getKey(), entry.getValue().getSize()));
    }

    long totalSize = 0;
    for (CompactionFile mfi : candidateFiles) {
      totalSize += mfi.size;
    }

    List<FileRef> files = new ArrayList<FileRef>();

    while (candidateFiles.size() > 1) {
      CompactionFile max = candidateFiles.last();
      if (max.size * ratio <= totalSize) {
        files.clear();
        for (CompactionFile mfi : candidateFiles) {
          files.add(mfi.file);
          if (files.size() >= maxFilesToCompact)
            break;
        }

        break;
      }
      totalSize -= max.size;
      candidateFiles.remove(max);
    }

    int totalFilesToCompact = 0;
    if (request.getFiles().size() > maxFilesPerTablet)
      totalFilesToCompact = request.getFiles().size() - maxFilesPerTablet + 1;

    totalFilesToCompact = Math.min(totalFilesToCompact, maxFilesToCompact);

    if (files.size() < totalFilesToCompact) {

      TreeMap<FileRef,Long> tfc = new TreeMap<FileRef,Long>();
      for (Entry<FileRef,DataFileValue> entry : request.getFiles().entrySet()) {
        tfc.put(entry.getKey(), entry.getValue().getSize());
      }
      tfc.keySet().removeAll(files);

      // put data in candidateFiles to sort it
      candidateFiles.clear();
      for (Entry<FileRef,Long> entry : tfc.entrySet())
        candidateFiles.add(new CompactionFile(entry.getKey(), entry.getValue()));

      for (CompactionFile mfi : candidateFiles) {
        files.add(mfi.file);
        if (files.size() >= totalFilesToCompact)
          break;
      }
    }

    return files;
  }

}
