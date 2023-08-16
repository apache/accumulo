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
package org.apache.accumulo.core.tasks;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iteratorsImpl.system.SystemIteratorUtil;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.InputFile;
import org.apache.accumulo.core.tabletserver.thrift.IteratorConfig;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.tasks.compaction.CompactionTask;
import org.apache.accumulo.core.tasks.thrift.Task;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class CompactionTasksSerializationTest {

  @Test
  public void testCompactionTask() throws Exception {

    TExternalCompactionJob job = new TExternalCompactionJob();

    job.setFateTxId(123456789L);
    job.setExtent(new KeyExtent(TableId.of("2B"), new Text("3"), new Text("2")).toThrift());
    job.setKind(TCompactionKind.valueOf(CompactionKind.SYSTEM.name()));
    List<InputFile> files = new ArrayList<>();
    files.add(new InputFile("hdfs://fake/path/to/output/fileA.rf", 32768, 1000,
        System.currentTimeMillis()));
    files.add(new InputFile("hdfs://fake/path/to/output/fileB.rf", 32768, 1000,
        System.currentTimeMillis()));
    job.setFiles(files);
    IteratorConfig iteratorSettings = SystemIteratorUtil.toIteratorConfig(List.of());
    job.setIteratorSettings(iteratorSettings);
    job.setOutputFile("hdfs://fake/path/to/output/fileC.rf");
    Map<String,String> overrides = new HashMap<>();
    overrides.put("override1", "value1");
    overrides.put("overrides2", "value2");
    job.setOverrides(overrides);
    job.setPropagateDeletes(true);

    CompactionTask task = new CompactionTask();
    task.setFateTxId(job.getFateTxId());
    task.setTaskId(UUID.randomUUID().toString());
    task.setCompactionJob(job);

    Task to = task.toThriftTask();
    assertEquals(TaskMessageType.COMPACTION_TASK.name(), to.getMessageType());
    System.out.println(to.getMessage());

    CompactionTask task2 = (CompactionTask) TaskMessage.fromThriftTask(to);

    assertEquals(task.getTaskId(), task2.getTaskId());
    assertEquals(task.getFateTxId(), task2.getFateTxId());
    assertEquals(task.getMessageType(), task2.getMessageType());
    assertEquals(task.getCompactionJob(), task2.getCompactionJob());
  }

}
