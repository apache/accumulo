package org.apache.accumulo.core.tasks;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.tabletserver.thrift.InputFile;
import org.apache.accumulo.core.tabletserver.thrift.IteratorConfig;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionKind;

public class CompactionTask extends BaseTask {

  private TKeyExtent extent;
  private List<InputFile> files;
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

}
