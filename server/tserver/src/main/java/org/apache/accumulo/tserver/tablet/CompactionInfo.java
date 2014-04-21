package org.apache.accumulo.tserver.tablet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.tabletserver.thrift.ActiveCompaction;
import org.apache.accumulo.core.tabletserver.thrift.CompactionReason;
import org.apache.accumulo.core.tabletserver.thrift.CompactionType;
import org.apache.accumulo.server.fs.FileRef;

public class CompactionInfo {

  private final Compactor compactor;
  private final String localityGroup;
  private final long entriesRead;
  private final long entriesWritten;

  CompactionInfo(Compactor compactor) {
    this.localityGroup = compactor.getCurrentLocalityGroup();
    this.entriesRead = compactor.getEntriesRead();
    this.entriesWritten = compactor.getEntriesWritten();
    this.compactor = compactor;
  }

  public long getID() {
    return compactor.getCompactorID();
  }

  public KeyExtent getExtent() {
    return compactor.getExtent();
  }

  public long getEntriesRead() {
    return entriesRead;
  }

  public long getEntriesWritten() {
    return entriesWritten;
  }

  public Thread getThread() {
    return compactor.thread;
  }

  public String getOutputFile() {
    return compactor.getOutputFile();
  }

  public ActiveCompaction toThrift() {

    CompactionType type;

    if (compactor.hasIMM())
      if (compactor.getFilesToCompact().size() > 0)
        type = CompactionType.MERGE;
      else
        type = CompactionType.MINOR;
    else if (!compactor.willPropogateDeletes())
      type = CompactionType.FULL;
    else
      type = CompactionType.MAJOR;

    CompactionReason reason;

    if (compactor.hasIMM())
      switch (compactor.getMinCReason()) {
        case USER:
          reason = CompactionReason.USER;
          break;
        case CLOSE:
          reason = CompactionReason.CLOSE;
          break;
        case SYSTEM:
        default:
          reason = CompactionReason.SYSTEM;
          break;
      }
    else
      switch (compactor.getMajorCompactionReason()) {
        case USER:
          reason = CompactionReason.USER;
          break;
        case CHOP:
          reason = CompactionReason.CHOP;
          break;
        case IDLE:
          reason = CompactionReason.IDLE;
          break;
        case NORMAL:
        default:
          reason = CompactionReason.SYSTEM;
          break;
      }

    List<IterInfo> iiList = new ArrayList<IterInfo>();
    Map<String,Map<String,String>> iterOptions = new HashMap<String,Map<String,String>>();

    for (IteratorSetting iterSetting : compactor.getIterators()) {
      iiList.add(new IterInfo(iterSetting.getPriority(), iterSetting.getIteratorClass(), iterSetting.getName()));
      iterOptions.put(iterSetting.getName(), iterSetting.getOptions());
    }
    List<String> filesToCompact = new ArrayList<String>();
    for (FileRef ref : compactor.getFilesToCompact())
      filesToCompact.add(ref.toString());
    return new ActiveCompaction(compactor.extent.toThrift(), System.currentTimeMillis() - compactor.getStartTime(), filesToCompact,
        compactor.getOutputFile(), type, reason, localityGroup, entriesRead, entriesWritten, iiList, iterOptions);
  }
}