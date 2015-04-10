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

import org.apache.accumulo.core.data.TabletID;
import org.apache.hadoop.io.Text;

import com.google.common.base.Function;

public class TabletIDImpl implements TabletID {

  private KeyExtent ke;

  @SuppressWarnings("deprecation")
  public static final Function<org.apache.accumulo.core.data.KeyExtent,TabletID> KE_2_TID_OLD = new Function<org.apache.accumulo.core.data.KeyExtent,TabletID>() {
    @Override
    public TabletID apply(org.apache.accumulo.core.data.KeyExtent input) {
      //the following if null check is to appease findbugs... grumble grumble spent a good part of my morning looking into this
      // http://sourceforge.net/p/findbugs/bugs/1139/
      // https://code.google.com/p/guava-libraries/issues/detail?id=920
      if(input == null)
        return null;
      return new TabletIDImpl(input);
    }
  };

  @SuppressWarnings("deprecation")
  public static final Function<TabletID, org.apache.accumulo.core.data.KeyExtent> TID_2_KE_OLD = new Function<TabletID, org.apache.accumulo.core.data.KeyExtent>() {
    @Override
    public org.apache.accumulo.core.data.KeyExtent apply(TabletID input) {
      if(input == null)
        return null;
      return new org.apache.accumulo.core.data.KeyExtent(input.getTableId(), input.getEndRow(), input.getPrevEndRow());
    }

  };

  @Deprecated
  public TabletIDImpl(org.apache.accumulo.core.data.KeyExtent ke) {
    this.ke = new KeyExtent(ke.getTableId(), ke.getEndRow(), ke.getPrevEndRow());
  }

  public TabletIDImpl(KeyExtent ke) {
    this.ke = ke;
  }

  @Override
  public int compareTo(TabletID o) {
    return ke.compareTo(((TabletIDImpl) o).ke);
  }

  @Override
  public Text getTableId() {
    return ke.getTableId();
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
    if (o instanceof TabletIDImpl) {
      return ke.equals(((TabletIDImpl) o).ke);
    }

    return false;
  }

  @Override
  public String toString(){
    return ke.toString();
  }
}
