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
package org.apache.accumulo.test.continuous;

import static com.google.common.base.Charsets.UTF_8;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

class HistData<T> implements Comparable<HistData<T>>, Serializable {
  private static final long serialVersionUID = 1L;

  T bin;
  long count;

  HistData(T bin) {
    this.bin = bin;
    count = 0;
  }

  @SuppressWarnings("unchecked")
  public int compareTo(HistData<T> o) {
    return ((Comparable<T>) bin).compareTo(o.bin);
  }
}

public class Histogram<T> implements Serializable {

  private static final long serialVersionUID = 1L;

  protected long sum;
  protected HashMap<T,HistData<T>> counts;

  public Histogram() {
    sum = 0;
    counts = new HashMap<T,HistData<T>>();
  }

  public void addPoint(T x) {
    addPoint(x, 1);
  }

  public void addPoint(T x, long y) {

    HistData<T> hd = counts.get(x);
    if (hd == null) {
      hd = new HistData<T>(x);
      counts.put(x, hd);
    }

    hd.count += y;
    sum += y;
  }

  public long getCount(T x) {
    HistData<T> hd = counts.get(x);
    if (hd == null)
      return 0;
    return hd.count;
  }

  public double getPercentage(T x) {
    if (getSum() == 0) {
      return 0;
    }
    return (double) getCount(x) / (double) getSum() * 100.0;
  }

  public long getSum() {
    return sum;
  }

  public List<T> getKeysInCountSortedOrder() {

    ArrayList<HistData<T>> sortedCounts = new ArrayList<HistData<T>>(counts.values());

    Collections.sort(sortedCounts, new Comparator<HistData<T>>() {
      public int compare(HistData<T> o1, HistData<T> o2) {
        if (o1.count < o2.count)
          return -1;
        if (o1.count > o2.count)
          return 1;
        return 0;
      }
    });

    ArrayList<T> sortedKeys = new ArrayList<T>();

    for (Iterator<HistData<T>> iter = sortedCounts.iterator(); iter.hasNext();) {
      HistData<T> hd = iter.next();
      sortedKeys.add(hd.bin);
    }

    return sortedKeys;
  }

  public void print(StringBuilder out) {
    TreeSet<HistData<T>> sortedCounts = new TreeSet<HistData<T>>(counts.values());

    int maxValueLen = 0;

    for (Iterator<HistData<T>> iter = sortedCounts.iterator(); iter.hasNext();) {
      HistData<T> hd = iter.next();
      if (("" + hd.bin).length() > maxValueLen) {
        maxValueLen = ("" + hd.bin).length();
      }
    }

    double psum = 0;

    for (Iterator<HistData<T>> iter = sortedCounts.iterator(); iter.hasNext();) {
      HistData<T> hd = iter.next();

      psum += getPercentage(hd.bin);

      out.append(String.format(" %" + (maxValueLen + 1) + "s %,16d %6.2f%s %6.2f%s%n", hd.bin + "", hd.count, getPercentage(hd.bin), "%", psum, "%"));
    }
    out.append(String.format("%n %" + (maxValueLen + 1) + "s %,16d %n", "TOTAL", sum));
  }

  public void save(String file) throws IOException {

    FileOutputStream fos = new FileOutputStream(file);
    BufferedOutputStream bos = new BufferedOutputStream(fos);
    PrintStream ps = new PrintStream(bos, false, UTF_8.name());

    TreeSet<HistData<T>> sortedCounts = new TreeSet<HistData<T>>(counts.values());
    for (Iterator<HistData<T>> iter = sortedCounts.iterator(); iter.hasNext();) {
      HistData<T> hd = iter.next();
      ps.println(" " + hd.bin + " " + hd.count);
    }

    ps.close();
  }

  public Set<T> getKeys() {
    return counts.keySet();
  }

  public void clear() {
    counts.clear();
    sum = 0;
  }
}
