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
import org.apache.accumulo.core.tasks.thrift.TaskObject;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class CompactionTaskSerializationTest {

  @Test
  public void testSerDeser() throws Exception {
    
    CompactionTask task = new CompactionTask();
    task.setTaskId(UUID.randomUUID().toString());
    task.setFateTxId(123456789L);
    task.setExtent(new KeyExtent(TableId.of("2B"), new Text("3"), new Text("2")).toThrift());
    task.setKind(TCompactionKind.valueOf(CompactionKind.SYSTEM.name()));
    List<InputFile> files = new ArrayList<>();
    files.add(new InputFile("hdfs://fake/path/to/output/fileA.rf", 32768, 1000, System.currentTimeMillis()));
    files.add(new InputFile("hdfs://fake/path/to/output/fileB.rf", 32768, 1000, System.currentTimeMillis()));
    task.setFiles(files);
    IteratorConfig iteratorSettings = SystemIteratorUtil
        .toIteratorConfig(List.of());
    task.setIteratorSettings(iteratorSettings);
    task.setOutputFile("hdfs://fake/path/to/output/fileC.rf");
    Map<String, String> overrides = new HashMap<>();
    overrides.put("override1", "value1");
    overrides.put("overrides2", "value2");
    task.setOverrides(overrides);
    task.setPropagateDeletes(true);

    TaskObject to = BaseTask.serialize(task);
    
    assertEquals(to.getTaskID(), task.getTaskId());
    assertEquals(to.getObjectType(), task.getClass().getName());
    System.out.println(to.getCborEncodedObject());
    
    CompactionTask task2 = BaseTask.deserialize(to);
    assertEquals(task, task2);
  }

  
}
