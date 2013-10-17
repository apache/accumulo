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
package org.apache.accumulo.server.tabletserver.compaction;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.server.fs.FileRef;

/**
 * A plan for a compaction: the passes to run over input files, producing output files to replace them,  
 * the files that are *not* inputs to a compaction that should simply be deleted, and weather or not to 
 * propagate deletes from input files to output files.
 */
public class CompactionPlan {
  public List<CompactionPass> passes = new ArrayList<CompactionPass>();
  public List<FileRef> deleteFiles = new ArrayList<FileRef>();
  public boolean propogateDeletes = true;
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append(passes.toString());
    if (!deleteFiles.isEmpty()) { 
      b.append(" files to be deleted ");
      b.append(deleteFiles);
    }
    b.append(" propogateDeletes ");
    b.append(propogateDeletes);
    return b.toString(); 
  }
}
