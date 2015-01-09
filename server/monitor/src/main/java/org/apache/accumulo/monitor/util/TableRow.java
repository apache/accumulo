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
package org.apache.accumulo.monitor.util;

import java.util.ArrayList;
import java.util.Comparator;

public class TableRow {
  private int size;
  private ArrayList<Object> row;

  TableRow(int size) {
    this.size = size;
    this.row = new ArrayList<Object>(size);
  }

  public boolean add(Object obj) {
    if (row.size() == size)
      throw new IllegalStateException("Row is full.");
    return row.add(obj);
  }

  Object get(int index) {
    return row.get(index);
  }

  int size() {
    return row.size();
  }

  Object set(int i, Object value) {
    return row.set(i, value);
  }

  public static <T> Comparator<TableRow> getComparator(int index, Comparator<T> comp) {
    return new TableRowComparator<T>(index, comp);
  }

  private static class TableRowComparator<T> implements Comparator<TableRow> {
    private int index;
    private Comparator<T> comp;

    public TableRowComparator(int index, Comparator<T> comp) {
      this.index = index;
      this.comp = comp;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compare(TableRow o1, TableRow o2) {
      return comp.compare((T) o1.get(index), (T) o2.get(index));
    }
  }
}
