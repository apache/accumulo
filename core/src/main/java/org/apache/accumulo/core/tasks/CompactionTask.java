package org.apache.accumulo.core.tasks;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.tabletserver.thrift.InputFile;
import org.apache.accumulo.core.tabletserver.thrift.IteratorConfig;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionKind;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

public class CompactionTask extends BaseTask {

  @JsonSerialize(using=ThriftSerializer.class, as=TKeyExtent.class)
  @JsonDeserialize(using=ThriftDeserializer.class, as=TKeyExtent.class)
  private TKeyExtent extent;

  private List<InputFile> files;

  @JsonSerialize(using=ThriftSerializer.class, as=IteratorConfig.class)
  @JsonDeserialize(using=ThriftDeserializer.class, as=IteratorConfig.class)
  private IteratorConfig iteratorSettings;

  private String outputFile;
  
  private boolean propagateDeletes;

  private TCompactionKind kind;

  private Map<String,String> overrides;

  public TKeyExtent getExtent() {
    return extent;
  }

  public void setExtent(TKeyExtent extent) {
    this.extent = extent;
  }

  public List<InputFile> getFiles() {
    return files;
  }

  public void setFiles(List<InputFile> files) {
    this.files = files;
  }

  public IteratorConfig getIteratorSettings() {
    return iteratorSettings;
  }

  public void setIteratorSettings(IteratorConfig iteratorSettings) {
    this.iteratorSettings = iteratorSettings;
  }

  public String getOutputFile() {
    return outputFile;
  }

  public void setOutputFile(String outputFile) {
    this.outputFile = outputFile;
  }

  public boolean isPropagateDeletes() {
    return propagateDeletes;
  }

  public void setPropagateDeletes(boolean propagateDeletes) {
    this.propagateDeletes = propagateDeletes;
  }

  public TCompactionKind getKind() {
    return kind;
  }

  public void setKind(TCompactionKind kind) {
    this.kind = kind;
  }

  public Map<String,String> getOverrides() {
    return overrides;
  }

  public void setOverrides(Map<String,String> overrides) {
    this.overrides = overrides;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((extent == null) ? 0 : extent.hashCode());
    result = prime * result + ((files == null) ? 0 : files.hashCode());
    result = prime * result + ((iteratorSettings == null) ? 0 : iteratorSettings.hashCode());
    result = prime * result + ((kind == null) ? 0 : kind.hashCode());
    result = prime * result + ((outputFile == null) ? 0 : outputFile.hashCode());
    result = prime * result + ((overrides == null) ? 0 : overrides.hashCode());
    result = prime * result + (propagateDeletes ? 1231 : 1237);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    CompactionTask other = (CompactionTask) obj;
    if (extent == null) {
      if (other.extent != null)
        return false;
    } else if (!extent.equals(other.extent))
      return false;
    if (files == null) {
      if (other.files != null)
        return false;
    } else if (!files.equals(other.files))
      return false;
    if (iteratorSettings == null) {
      if (other.iteratorSettings != null)
        return false;
    } else if (!iteratorSettings.equals(other.iteratorSettings))
      return false;
    if (kind != other.kind)
      return false;
    if (outputFile == null) {
      if (other.outputFile != null)
        return false;
    } else if (!outputFile.equals(other.outputFile))
      return false;
    if (overrides == null) {
      if (other.overrides != null)
        return false;
    } else if (!overrides.equals(other.overrides))
      return false;
    if (propagateDeletes != other.propagateDeletes)
      return false;
    return true;
  }

  
}
