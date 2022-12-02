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
package org.apache.accumulo.core.clientImpl.bulk;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public class Bulk {

  /**
   * WARNING : do not change this class, its used for serialization to Json
   */
  public static class Mapping {
    private Tablet tablet;
    private Collection<FileInfo> files;

    public Mapping(KeyExtent tablet, Files files) {
      this.tablet = toTablet(tablet);
      this.files = files.files.values();
    }

    public Tablet getTablet() {
      return tablet;
    }

    public KeyExtent getKeyExtent(TableId tableId) {
      return tablet.toKeyExtent(tableId);
    }

    public Files getFiles() {
      return new Files(files);
    }
  }

  /**
   * WARNING : do not change this class, its used for serialization to Json
   */
  public static class Tablet {

    private byte[] endRow;
    private byte[] prevEndRow;

    public Tablet(Text endRow, Text prevEndRow) {
      this.endRow = endRow == null ? null : TextUtil.getBytes(endRow);
      this.prevEndRow = prevEndRow == null ? null : TextUtil.getBytes(prevEndRow);
    }

    public KeyExtent toKeyExtent(TableId tableId) {
      return Bulk.toKeyExtent(tableId, this);
    }

    public Text getEndRow() {
      if (endRow == null) {
        return null;
      }
      return new Text(endRow);
    }

    public Text getPrevEndRow() {
      if (prevEndRow == null) {
        return null;
      }
      return new Text(prevEndRow);
    }

    @Override
    public String toString() {
      return getEndRow() + ";" + getPrevEndRow();
    }
  }

  /**
   * WARNING : do not change this class, its used for serialization to Json
   */
  public static class FileInfo {
    final String name;
    final long estSize;
    final long estEntries;

    public FileInfo(String fileName, long estFileSize, long estNumEntries) {
      this.name = fileName;
      this.estSize = estFileSize;
      this.estEntries = estNumEntries;
    }

    public FileInfo(Path path, long estSize) {
      this(path.getName(), estSize, 0);
    }

    static FileInfo merge(FileInfo fi1, FileInfo fi2) {
      Preconditions.checkArgument(fi1.name.equals(fi2.name));
      return new FileInfo(fi1.name, fi1.estSize + fi2.estSize, fi1.estEntries + fi2.estEntries);
    }

    public String getFileName() {
      return name;
    }

    public long getEstFileSize() {
      return estSize;
    }

    public long getEstNumEntries() {
      return estEntries;
    }

    @Override
    public String toString() {
      return String.format("file:%s  estSize:%d estEntries:%s", name, estSize, estEntries);
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof FileInfo)) {
        return false;
      }
      FileInfo other = (FileInfo) o;
      return this.name.equals(other.name) && this.estSize == other.estSize
          && this.estEntries == other.estEntries;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, estSize, estEntries);
    }
  }

  public static class Files implements Iterable<FileInfo> {
    Map<String,FileInfo> files = new HashMap<>();

    public Files(Collection<FileInfo> files) {
      files.forEach(this::add);
    }

    public Files() {}

    public void add(FileInfo fi) {
      if (files.putIfAbsent(fi.name, fi) != null) {
        throw new IllegalArgumentException("File already present " + fi.name);
      }
    }

    public FileInfo get(String fileName) {
      return files.get(fileName);
    }

    public Files mapNames(Map<String,String> renames) {
      Files renamed = new Files();

      files.forEach((k, v) -> {
        String newName = renames.get(k);
        FileInfo nfi = new FileInfo(newName, v.estSize, v.estEntries);
        renamed.files.put(newName, nfi);
      });

      return renamed;
    }

    void merge(Files other) {
      other.files.forEach((k, v) -> {
        files.merge(k, v, FileInfo::merge);
      });
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
      if (o == this) {
        return true;
      }
      if (!(o instanceof Files)) {
        return false;
      }
      Files other = (Files) o;
      return this.files.equals(other.files);
    }

    @Override
    public int hashCode() {
      return files.hashCode();
    }

    @Override
    public String toString() {
      return files.toString();
    }
  }

  public static Tablet toTablet(KeyExtent keyExtent) {
    return new Tablet(keyExtent.endRow(), keyExtent.prevEndRow());
  }

  public static KeyExtent toKeyExtent(TableId tableId, Tablet tablet) {
    return new KeyExtent(tableId, tablet.getEndRow(), tablet.getPrevEndRow());
  }
}
