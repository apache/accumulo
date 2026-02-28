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
package org.apache.accumulo.monitor.next.ec;

import java.util.Comparator;
import java.util.List;

import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.tabletserver.thrift.InputFile;
import org.apache.accumulo.core.util.compaction.RunningCompactionInfo;

public class RunningCompactionDetails extends RunningCompactionInfo {

  // Variable names become JSON keys
  public final List<CompactionInputFileDetails> inputFiles;
  public final String outputFile;

  public RunningCompactionDetails(TExternalCompaction ec) {
    super(ec);
    var job = ec.getJob();
    this.inputFiles = convertInputFiles(job.files);
    this.outputFile = job.outputFile;
  }

  /**
   * @return a list of {@link CompactionInputFileDetails} sorted largest to smallest
   */
  private List<CompactionInputFileDetails> convertInputFiles(List<InputFile> files) {
    return files.stream()
        .map(file -> new CompactionInputFileDetails(file.metadataFileEntry, file.size, file.entries,
            file.timestamp))
        .sorted(Comparator.comparingLong(CompactionInputFileDetails::size).reversed()).toList();
  }
}
