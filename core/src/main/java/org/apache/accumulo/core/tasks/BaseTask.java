package org.apache.accumulo.core.tasks;

import java.io.IOException;

import org.apache.accumulo.core.tasks.thrift.TaskObject;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;

@JsonTypeInfo(use=Id.CLASS, include=As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value=CompactionTask.class, name="CompactionTask")
})
public abstract class BaseTask {
  
  // thread-safe if configured before any read/write calls
  private static final CBORMapper mapper = new CBORMapper();

  static {
    mapper.configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true);
    mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
  }

  public static <T extends BaseTask> TaskObject serialize(T task) throws JsonProcessingException {
    TaskObject to = new TaskObject();
    to.setTaskID(task.getTaskId());
    to.setObjectType(task.getClass().getName());
    to.setCborEncodedObject(mapper.writeValueAsBytes(task));
    return to;
  }

  @SuppressWarnings("unchecked")
  public static <T extends BaseTask> T deserialize(TaskObject to)
      throws ClassNotFoundException, StreamReadException, DatabindException, IOException {
    Class<? extends BaseTask> clazz = (Class<? extends BaseTask>) Class.forName(to.getObjectType());
    return (T) mapper.readValue(to.getCborEncodedObject(), clazz);
  }

  private String taskId;
  private long fateTxId;

  public BaseTask() {}

  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  public long getFateTxId() {
    return fateTxId;
  }

  public void setFateTxId(long fateTxId) {
    this.fateTxId = fateTxId;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (fateTxId ^ (fateTxId >>> 32));
    result = prime * result + ((taskId == null) ? 0 : taskId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    BaseTask other = (BaseTask) obj;
    if (fateTxId != other.fateTxId)
      return false;
    if (taskId == null) {
      if (other.taskId != null)
        return false;
    } else if (!taskId.equals(other.taskId))
      return false;
    return true;
  }

}
