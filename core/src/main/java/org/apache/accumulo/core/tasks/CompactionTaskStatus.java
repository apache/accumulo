package org.apache.accumulo.core.tasks;

import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.thrift.TException;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class CompactionTaskStatus extends Task implements ThriftSerialization<TExternalCompaction> {

  public CompactionTaskStatus() {
    this.setType(TaskType.COMPACTION_STATUS);
  }

  @JsonIgnore
  public void setCompactionStatus(TExternalCompaction status) throws TException {
    this.setSerializedThriftObject(this.serialize(status));
  }

  @JsonIgnore
  public TExternalCompaction getCompactionStatus() throws TException {
    TExternalCompaction status = new TExternalCompaction();
    this.deserialize(status, this.getSerializedThriftObject());
    return status;
  }
  
}
