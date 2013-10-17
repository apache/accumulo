package org.apache.accumulo.server.tabletserver.compaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.fs.FileRef;

public class DefaultCompactionStrategy implements CompactionStrategy {
  
  Map<FileRef,Pair<Key,Key>> firstAndLastKeys = null;
  
  @Override
  public void gatherInformation(MajorCompactionRequest request) throws IOException {
    if (request.getReason() == MajorCompactionReason.CHOP) {
      firstAndLastKeys = getFirstAndLastKeys(request);
    }
  }
@Override
  public CompactionPlan getCompactionPlan(MajorCompactionRequest request) throws IOException {
    CompactionPlan result = new CompactionPlan();
    
    List<FileRef> toCompact;
    MajorCompactionReason reason = request.getReason();
    if (reason == MajorCompactionReason.CHOP) {
      toCompact = findChopFiles(request);
    } else {
      toCompact = findMapFilesToCompact(request);
    }
    CompactionPass pass = new CompactionPass();
    pass.inputFiles = toCompact;
    if (toCompact == null || toCompact.isEmpty())
      return result;
    result.passes.add(pass);
    result.propogateDeletes = toCompact.size() != request.getFiles().size();
    return result;
  }
  
  private Map<FileRef,Pair<Key,Key>> getFirstAndLastKeys(MajorCompactionRequest request) throws IOException {
    Map<FileRef,Pair<Key,Key>> result = new HashMap<FileRef,Pair<Key,Key>>();
    
    for (Entry<FileRef,DataFileValue> entry : request.getFiles().entrySet()) {
      FileRef file = entry.getKey();
      FileSKVIterator openReader = request.openReader(file);
      try {
        Key first = openReader.getFirstKey();
        Key last = openReader.getLastKey();
        result.put(file, new Pair<Key,Key>(first, last));
      } finally {
        openReader.close();
      }
    }
    return result;
  }

  
  List<FileRef> findChopFiles(MajorCompactionRequest request) throws IOException {
    List<FileRef> result = new ArrayList<FileRef>();
    if (firstAndLastKeys == null) {
      // someone called getCompactionPlan without calling gatherInformation: compact everything
      result.addAll(request.getFiles().keySet());
      return result;
    } 
    
    for (Entry<FileRef,DataFileValue> entry : request.getFiles().entrySet()) {
      FileRef file = entry.getKey();
      Pair<Key,Key> pair = firstAndLastKeys.get(file);
      if (pair == null) {
        // file was created or imported after we obtained the first and last keys... there
        // are a few options here... throw an exception which will cause the compaction to
        // retry and also cause ugly error message that the admin has to ignore... could
        // go get the first and last key, but this code is called while the tablet lock
        // is held... or just compact the file....
        result.add(file);
      } else {
        Key first = pair.getFirst();
        Key last = pair.getSecond();
        // If first and last are null, it's an empty file. Add it to the compact set so it goes away.
        KeyExtent extent = request.getExtent();
        if ((first == null && last == null) || !extent.contains(first.getRow()) || !extent.contains(last.getRow())) {
          result.add(file);
        }
      }
    }
    return result;
  }
  
  private static class CompactionFile {
    public FileRef file;
    public long size;
    public CompactionFile(FileRef file, long size) {
      super();
      this.file = file;
      this.size = size;
    }
  }

  
  private List<FileRef> findMapFilesToCompact(MajorCompactionRequest request) {
    MajorCompactionReason reason = request.getReason();
    if (reason == MajorCompactionReason.USER) {
      return new ArrayList<FileRef>(request.getFiles().keySet());
    }
    
    if (request.getFiles().size() <= 1)
      return null;
    TreeSet<CompactionFile> candidateFiles = new TreeSet<CompactionFile>(new Comparator<CompactionFile>() {
      @Override
      public int compare(CompactionFile o1, CompactionFile o2) {
        if (o1 == o2)
          return 0;
        if (o1.size < o2.size)
          return -1;
        if (o1.size > o2.size)
          return 1;
        return o1.file.compareTo(o2.file);
      }
    });
    
    double ratio = Double.parseDouble(request.getTableConfig(Property.TABLE_MAJC_RATIO.getKey()));
    int maxFilesToCompact = Integer.parseInt(request.getTableConfig(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey()));
    int maxFilesPerTablet = request.getMaxFilesPerTablet();
    
    for (Entry<FileRef,DataFileValue> entry : request.getFiles().entrySet()) {
      candidateFiles.add(new CompactionFile(entry.getKey(), entry.getValue().getSize()));
    }
    
    long totalSize = 0;
    for (CompactionFile mfi : candidateFiles) {
      totalSize += mfi.size;
    }
    
    List<FileRef> files = new ArrayList<FileRef>();
    
    while (candidateFiles.size() > 1) {
      CompactionFile max = candidateFiles.last();
      if (max.size * ratio <= totalSize) {
        files.clear();
        for (CompactionFile mfi : candidateFiles) {
          files.add(mfi.file);
          if (files.size() >= maxFilesToCompact)
            break;
        }
        
        break;
      }
      totalSize -= max.size;
      candidateFiles.remove(max);
    }
    
    int totalFilesToCompact = 0;
    if (request.getFiles().size() > maxFilesPerTablet)
      totalFilesToCompact = request.getFiles().size() - maxFilesPerTablet + 1;
    
    totalFilesToCompact = Math.min(totalFilesToCompact, maxFilesToCompact);
    
    if (files.size() < totalFilesToCompact) {
      
      TreeMap<FileRef,Long> tfc = new TreeMap<FileRef,Long>();
      for (Entry<FileRef,DataFileValue> entry : request.getFiles().entrySet()) {
          tfc.put(entry.getKey(), entry.getValue().getSize());
      }
      tfc.keySet().removeAll(files);
      
      // put data in candidateFiles to sort it
      candidateFiles.clear();
      for (Entry<FileRef,Long> entry : tfc.entrySet())
        candidateFiles.add(new CompactionFile(entry.getKey(), entry.getValue()));
      
      for (CompactionFile mfi : candidateFiles) {
        files.add(mfi.file);
        if (files.size() >= totalFilesToCompact)
          break;
      }
    }
    
    return files;
  }
  @Override
  public Writer getCompactionWriter() {
    return new DefaultWriter();
  }
  
}
