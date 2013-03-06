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
package org.apache.accumulo.core.util.shell.commands;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.ActiveCompaction;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.util.Duration;

class ActiveCompactionIterator implements Iterator<String> {

  private InstanceOperations instanceOps;
  private Iterator<String> tsIter;
  private Iterator<String> compactionIter;

  private static String maxDecimal(double count) {
    if (count < 9.995)
      return String.format("%.2f", count);
    if (count < 99.95)
      return String.format("%.1f", count);
    return String.format("%.0f", count);
  }

  private static String shortenCount(long count) {
    if (count < 1000)
      return count + "";
    if (count < 1000000)
      return maxDecimal(count / 1000.0) + "K";
    if (count < 1000000000)
      return maxDecimal(count / 1000000.0) + "M";
    return maxDecimal(count / 1000000000.0) + "B";
  }

  private void readNext() {
    final List<String> compactions = new ArrayList<String>();

    while (tsIter.hasNext()) {

      final String tserver = tsIter.next();
      try {
        List<ActiveCompaction> acl = instanceOps.getActiveCompactions(tserver);

        acl = new ArrayList<ActiveCompaction>(acl);

        Collections.sort(acl, new Comparator<ActiveCompaction>() {
          @Override
          public int compare(ActiveCompaction o1, ActiveCompaction o2) {
            return (int) (o2.getAge() - o1.getAge());
          }
        });

        for (ActiveCompaction ac : acl) {
          String output = ac.getOutputFile();
          int index = output.indexOf("tables");
          if (index > 0) {
            output = output.substring(index + 6);
          }

          ac.getIterators();

          List<String> iterList = new ArrayList<String>();
          Map<String,Map<String,String>> iterOpts = new HashMap<String,Map<String,String>>();
          for (IteratorSetting is : ac.getIterators()) {
            iterList.add(is.getName() + "=" + is.getPriority() + "," + is.getIteratorClass());
            iterOpts.put(is.getName(), is.getOptions());
          }

          compactions.add(String.format("%21s | %9s | %5s | %6s | %5s | %5s | %15s | %-40s | %5s | %35s | %9s | %s", tserver,
              Duration.format(ac.getAge(), "", "-"), ac.getType(), ac.getReason(), shortenCount(ac.getEntriesRead()), shortenCount(ac.getEntriesWritten()),
              ac.getTable(), ac.getExtent(), ac.getInputFiles().size(), output, iterList, iterOpts));
        }
      } catch (Exception e) {
        compactions.add(tserver + " ERROR " + e.getMessage());
      }

      if (compactions.size() > 0) {
        break;
      }
    }

    compactionIter = compactions.iterator();
  }

  ActiveCompactionIterator(List<String> tservers, InstanceOperations instanceOps) {
    this.instanceOps = instanceOps;
    this.tsIter = tservers.iterator();

    final String header = String.format(" %-21s| %-9s | %-5s | %-6s | %-5s | %-5s | %-15s | %-40s | %-5s | %-35s | %-9s | %s", "TABLET SERVER", "AGE", "TYPE",
        "REASON", "READ", "WROTE", "TABLE", "TABLET", "INPUT", "OUTPUT", "ITERATORS", "ITERATOR OPTIONS");

    compactionIter = Collections.singletonList(header).iterator();
  }

  @Override
  public boolean hasNext() {
    return compactionIter.hasNext();
  }

  @Override
  public String next() {
    final String next = compactionIter.next();

    if (!compactionIter.hasNext())
      readNext();

    return next;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
