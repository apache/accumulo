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
package org.apache.accumulo.core.client.impl;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public class Bulk {

  /**
   * Class used for Bulk serialization to Json
   */
  public static class Mapping implements Comparable<Mapping> {
    private Tablet tablet;
    private Files files;

    public Mapping(KeyExtent tablet, Files files) {
      this.tablet = toTablet(tablet);
      this.files = files;
    }

    @Override
    public String toString() {
      return tablet.toString() + " " + Arrays.toString(this.files.getAllFileNames().toArray());
    }

    public Tablet getTablet() {
      return tablet;
    }

    public KeyExtent getKeyExtent() {
      return tablet.toKeyExtent();
    }

    public Files getFiles() {
      return files;
    }

    @Override
    public int compareTo(Mapping o) {
      return this.tablet.compareTo(o.tablet);
    }

    @Override
    public boolean equals(Object o) {
      if (o == this)
        return true;
      if (!(o instanceof Mapping))
        return false;
      Mapping other = (Mapping) o;
      return this.tablet.equals(other.tablet) && this.files.equals(other.files);
    }

    private int hashcode = 0;

    @Override
    public int hashCode() {
      if (hashcode != 0)
        return hashcode;
      return hashcode = tablet.hashCode() + files.hashCode();
    }
  }

  /**
   * Basically a copy of KeyExtent that is used for Bulk serialization
   */
  public static class Tablet implements Comparable<Tablet> {
    private Table.ID tableId;
    private Text endRow;
    private Text prevEndRow;

    public Tablet(Table.ID tableId, Text endRow, Text prevEndRow) {
      this.tableId = tableId;
      this.endRow = endRow;
      this.prevEndRow = prevEndRow;
    }

    public KeyExtent toKeyExtent() {
      return Bulk.toKeyExtent(this);
    }

    public Table.ID getTableId() {
      return tableId;
    }

    public Text getEndRow() {
      return endRow;
    }

    public Text getPrevEndRow() {
      return prevEndRow;
    }

    @Override
    public String toString() {
      return tableId.canonicalID() + ";" + endRow.toString() + ";" + prevEndRow.toString();
    }

    // copied from KeyExtent
    @Override
    public int compareTo(Tablet other) {

      int result = getTableId().compareTo(other.getTableId());
      if (result != 0)
        return result;

      if (this.getEndRow() == null) {
        if (other.getEndRow() != null)
          return 1;
      } else {
        if (other.getEndRow() == null)
          return -1;

        result = getEndRow().compareTo(other.getEndRow());
        if (result != 0)
          return result;
      }
      if (this.getPrevEndRow() == null) {
        if (other.getPrevEndRow() == null)
          return 0;
        return -1;
      }
      if (other.getPrevEndRow() == null)
        return 1;
      return this.getPrevEndRow().compareTo(other.getPrevEndRow());
    }

    @Override
    public boolean equals(Object o) {
      if (o == this)
        return true;
      if (!(o instanceof Tablet))
        return false;
      Tablet other = (Tablet) o;
      return tableId.equals(other.tableId) && equals(endRow, other.getEndRow())
          && equals(prevEndRow, other.getPrevEndRow());
    }

    private boolean equals(Text t1, Text t2) {
      if (t1 == null || t2 == null)
        return t1 == t2;
      return t1.equals(t2);
    }

    private int hashCode = 0;

    @Override
    public int hashCode() {
      if (hashCode != 0)
        return hashCode;

      int prevEndRowHash = 0;
      int endRowHash = 0;
      if (this.getEndRow() != null) {
        endRowHash = this.getEndRow().hashCode();
      }

      if (this.getPrevEndRow() != null) {
        prevEndRowHash = this.getPrevEndRow().hashCode();
      }

      hashCode = getTableId().hashCode() + endRowHash + prevEndRowHash;
      return hashCode;
    }
  }

  public static class FileInfo implements Serializable {
    final String fileName;
    final long estFileSize;
    final long estNumEntries;

    public FileInfo(String fileName, long estFileSize, long estNumEntries) {
      this.fileName = fileName;
      this.estFileSize = estFileSize;
      this.estNumEntries = estNumEntries;
    }

    public FileInfo(Path path, long estSize) {
      this(path.getName(), estSize, 0);
    }

    static FileInfo merge(FileInfo fi1, FileInfo fi2) {
      Preconditions.checkArgument(fi1.fileName.equals(fi2.fileName));
      return new FileInfo(fi1.fileName, fi1.estFileSize + fi2.estFileSize,
          fi1.estNumEntries + fi2.estNumEntries);
    }

    public String getFileName() {
      return fileName;
    }

    public long getEstFileSize() {
      return estFileSize;
    }

    public long getEstNumEntries() {
      return estNumEntries;
    }

    @Override
    public String toString() {
      return String.format("file:%s  estSize:%d estEntries:%s", fileName, estFileSize,
          estNumEntries);
    }

    @Override
    public boolean equals(Object o) {
      if (o == this)
        return true;
      if (!(o instanceof FileInfo))
        return false;
      FileInfo other = (FileInfo) o;
      return this.fileName.equals(other.fileName) && this.estFileSize == other.estFileSize
          && this.estNumEntries == other.estNumEntries;
    }

    private int hashcode = 0;

    @Override
    public int hashCode() {
      if (hashcode != 0)
        return hashcode;
      return hashcode = fileName.hashCode() + Long.valueOf(estFileSize).hashCode()
          + Long.valueOf(estNumEntries).hashCode();
    }
  }

  public static class Files implements Iterable<FileInfo>, Serializable {
    Map<String,FileInfo> files = new HashMap<>();

    public void add(FileInfo fi) {
      if (files.putIfAbsent(fi.fileName, fi) != null) {
        throw new IllegalArgumentException("File already present " + fi.fileName);
      }
    }

    public FileInfo get(String fileName) {
      return files.get(fileName);
    }

    public Files mapNames(Map<String,String> renames) {
      Files renamed = new Files();

      files.forEach((k, v) -> {
        String newName = renames.get(k);
        FileInfo nfi = new FileInfo(newName, v.estFileSize, v.estNumEntries);
        renamed.files.put(newName, nfi);
      });

      return renamed;
    }

    void merge(Files other) {
      other.files.forEach((k, v) -> {
        files.merge(k, v, FileInfo::merge);
      });
    }

    public Set<String> getAllFileNames() {
      return this.files.keySet();
    }

    public int getSize() {
      return this.files.size();
    }

    @Override
    public Iterator<FileInfo> iterator() {
      return files.values().iterator();
    }

    @Override
    public boolean equals(Object o) {
      if (o == this)
        return true;
      if (!(o instanceof Files))
        return false;
      Files other = (Files) o;
      return this.files.equals(other.files);
    }

    private int hashcode = 0;

    @Override
    public int hashCode() {
      if (hashcode != 0)
        return hashcode;
      return hashcode = files.hashCode();
    }
  }

  public static Tablet toTablet(KeyExtent keyExtent) {
    return new Tablet(keyExtent.getTableId(), keyExtent.getEndRow(), keyExtent.getPrevEndRow());
  }

  public static KeyExtent toKeyExtent(Tablet tablet) {
    return new KeyExtent(tablet.getTableId(), tablet.getEndRow(), tablet.getPrevEndRow());
  }

  public static SortedMap<KeyExtent,Files> mapNames(SortedMap<KeyExtent,Files> mappings,
      Map<String,String> renames) {
    SortedMap<KeyExtent,Bulk.Files> newMappings = new TreeMap<>();
    mappings.forEach((k, f) -> newMappings.put(k, f.mapNames(renames)));
    return newMappings;
  }
}
