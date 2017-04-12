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
package org.apache.accumulo.tserver.log;

import java.io.EOFException;
import java.io.IOException;
import java.util.Objects;

import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.log.SortedLogState;
import org.apache.commons.collections.buffer.PriorityBuffer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Provide simple Map.Reader methods over multiple Maps.
 *
 * Presently only supports next() and seek() and works on all the Map directories within a directory. The primary purpose of this class is to merge the results
 * of multiple Reduce jobs that result in Map output files.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class MultiReader {

  /**
   * Group together the next key/value from a Reader with the Reader
   *
   */
  private static class Index implements Comparable<Index> {
    Reader reader;
    WritableComparable key;
    Writable value;
    boolean cached = false;

    private static Object create(java.lang.Class<?> klass) {
      try {
        return klass.getConstructor().newInstance();
      } catch (Throwable t) {
        throw new RuntimeException("Unable to construct objects to use for comparison");
      }
    }

    public Index(Reader reader) {
      this.reader = reader;
      key = (WritableComparable) create(reader.getKeyClass());
      value = (Writable) create(reader.getValueClass());
    }

    private void cache() throws IOException {
      if (!cached && reader.next(key, value)) {
        cached = true;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(key);
    }

    @Override
    public boolean equals(Object obj) {
      return this == obj || (obj != null && obj instanceof Index && 0 == compareTo((Index) obj));
    }

    @Override
    public int compareTo(Index o) {
      try {
        cache();
        o.cache();
        // no more data: always goes to the end
        if (!cached)
          return 1;
        if (!o.cached)
          return -1;
        return key.compareTo(o.key);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  private PriorityBuffer heap = new PriorityBuffer();

  public MultiReader(VolumeManager fs, Path directory) throws IOException {
    boolean foundFinish = false;
    for (FileStatus child : fs.listStatus(directory)) {
      if (child.getPath().getName().startsWith("_"))
        continue;
      if (SortedLogState.isFinished(child.getPath().getName())) {
        foundFinish = true;
        continue;
      }
      FileSystem ns = fs.getVolumeByPath(child.getPath()).getFileSystem();
      heap.add(new Index(new Reader(ns.makeQualified(child.getPath()), ns.getConf())));
    }
    if (!foundFinish)
      throw new IOException("Sort \"" + SortedLogState.FINISHED.getMarker() + "\" flag not found in " + directory);
  }

  private static void copy(Writable src, Writable dest) throws IOException {
    // not exactly efficient...
    DataOutputBuffer output = new DataOutputBuffer();
    src.write(output);
    DataInputBuffer input = new DataInputBuffer();
    input.reset(output.getData(), output.getLength());
    dest.readFields(input);
  }

  public synchronized boolean next(WritableComparable key, Writable val) throws IOException {
    Index elt = (Index) heap.remove();
    try {
      elt.cache();
      if (elt.cached) {
        copy(elt.key, key);
        copy(elt.value, val);
        elt.cached = false;
      } else {
        return false;
      }
    } finally {
      heap.add(elt);
    }
    return true;
  }

  public synchronized boolean seek(WritableComparable key) throws IOException {
    PriorityBuffer reheap = new PriorityBuffer(heap.size());
    boolean result = false;
    for (Object obj : heap) {
      Index index = (Index) obj;
      try {
        WritableComparable found = index.reader.getClosest(key, index.value, true);
        if (found != null && found.equals(key)) {
          result = true;
        }
      } catch (EOFException ex) {
        // thrown if key is beyond all data in the map
      }
      index.cached = false;
      reheap.add(index);
    }
    heap = reheap;
    return result;
  }

  public void close() throws IOException {
    IOException problem = null;
    for (Object obj : heap) {
      Index index = (Index) obj;
      try {
        index.reader.close();
      } catch (IOException ex) {
        problem = ex;
      }
    }
    if (problem != null)
      throw problem;
    heap = null;
  }

}
