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
package org.apache.accumulo.monitor.rest.compactions.external;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.tabletserver.thrift.InputFile;
import org.apache.accumulo.core.util.compaction.RunningCompactionInfo;

public class RunningCompactorDetails extends RunningCompactionInfo {
  // Variable names become JSON keys
  public final List<CompactionInputFile> inputFiles;
  public final String outputFile;

  public RunningCompactorDetails(TExternalCompaction ec) {
    super(ec);
    var job = ec.getJob();
    inputFiles = convertInputFiles(job.files);
    outputFile = job.outputFile;
  }

  private List<CompactionInputFile> convertInputFiles(List<InputFile> files) {
    List<CompactionInputFile> list = new ArrayList<>();
    files.forEach(f -> list
        .add(new CompactionInputFile(f.metadataFileEntry, f.size, f.entries, f.timestamp)));
    // sorted largest to smallest
    list.sort((o1, o2) -> Long.compare(o2.size, o1.size));
    return list;
  }
}
