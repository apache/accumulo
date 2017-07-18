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

package org.apache.accumulo.core.data.impl;

import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TabletId;
import org.apache.hadoop.io.Text;

public class TabletIdImpl implements TabletId {

  private KeyExtent ke;

  public TabletIdImpl(KeyExtent ke) {
    this.ke = ke;
  }

  @Override
  public int compareTo(TabletId o) {
    return ke.compareTo(((TabletIdImpl) o).ke);
  }

  @Override
  public Text getTableId() {
    return new Text(ke.getTableId().getUtf8());
  }

  @Override
  public Text getEndRow() {
    return ke.getEndRow();
  }

  @Override
  public Text getPrevEndRow() {
    return ke.getPrevEndRow();
  }

  @Override
  public int hashCode() {
    return ke.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof TabletIdImpl) {
      return ke.equals(((TabletIdImpl) o).ke);
    }

    return false;
  }

  @Override
  public String toString() {
    return ke.toString();
  }

  @Override
  public Range toRange() {
    return ke.toDataRange();
  }
}
