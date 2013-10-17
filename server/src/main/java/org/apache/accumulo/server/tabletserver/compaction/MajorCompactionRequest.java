package org.apache.accumulo.server.tabletserver.compaction;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Information that can be used to determine how a tablet is to be major compacted, if needed.
 */
public class MajorCompactionRequest {
  final private KeyExtent extent;
  final private MajorCompactionReason reason;
  final private VolumeManager volumeManager;
  final private AccumuloConfiguration tableConfig;
  private Map<FileRef,DataFileValue> files;
  
  public MajorCompactionRequest(
      KeyExtent extent, 
      MajorCompactionReason reason, 
      VolumeManager manager, 
      AccumuloConfiguration tabletConfig) {
    this.extent = extent;
    this.reason = reason;
    this.volumeManager = manager;
    this.tableConfig = tabletConfig;
    this.files = Collections.emptyMap();
  }
  
  public KeyExtent getExtent() {
    return extent;
  }
  
  public MajorCompactionReason getReason() {
    return reason;
  }
  
  public Map<FileRef,DataFileValue> getFiles() {
    return files;
  }
  
  public void setFiles(Map<FileRef,DataFileValue> update) {
    this.files = Collections.unmodifiableMap(update);
  }
  
  FileStatus[] listStatus(Path path) throws IOException {
    // @TODO verify the file isn't some random file in HDFS
    return volumeManager.listStatus(path);
  }
  
  FileSKVIterator openReader(FileRef ref) throws IOException {
    // @TODO verify the file isn't some random file in HDFS
    // @TODO ensure these files are always closed?
    FileOperations fileFactory = FileOperations.getInstance();
    FileSystem ns = volumeManager.getFileSystemByPath(ref.path());
    FileSKVIterator openReader = fileFactory.openReader(ref.path().toString(), true, ns, ns.getConf(), tableConfig);
    return openReader;
  }
  
  public Map<String,String> getTableProperties() {
    return tableConfig.getAllPropertiesWithPrefix(Property.TABLE_PREFIX);
  }

  public String getTableConfig(String key) {
    Property property = Property.getPropertyByKey(key);
    if (property == null || property.isSensitive())
      throw new RuntimeException("Unable to access the configuration value " + key);
    return tableConfig.get(property);
  }
  
  public int getMaxFilesPerTablet() {
    return tableConfig.getMaxFilesPerTablet();
  }
}
