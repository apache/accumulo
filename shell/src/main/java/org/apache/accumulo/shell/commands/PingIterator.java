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
package org.apache.accumulo.shell.commands;

import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.admin.InstanceOperations;

class PingIterator implements Iterator<String> {

  private Iterator<String> iter;
  private InstanceOperations instanceOps;

  PingIterator(List<String> tservers, InstanceOperations instanceOps) {
    iter = tservers.iterator();
    this.instanceOps = instanceOps;
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public String next() {
    String tserver = iter.next();

    try {
      instanceOps.ping(tserver);
      return tserver + " OK " + instanceOps.getTabletServerVersion(tserver);
    } catch (AccumuloException e) {
      return tserver + " ERROR " + e.getMessage();
    }

  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
