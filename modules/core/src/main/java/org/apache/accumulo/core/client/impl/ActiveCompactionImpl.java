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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ActiveCompaction;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.impl.TabletIdImpl;
import org.apache.accumulo.core.data.thrift.IterInfo;

/**
 *
 * @since 1.6.0
 */
public class ActiveCompactionImpl extends ActiveCompaction {

  private org.apache.accumulo.core.tabletserver.thrift.ActiveCompaction tac;
  private Instance instance;

  ActiveCompactionImpl(Instance instance, org.apache.accumulo.core.tabletserver.thrift.ActiveCompaction tac) {
    this.tac = tac;
    this.instance = instance;
  }

  @Override
  public String getTable() throws TableNotFoundException {
    return Tables.getTableName(instance, new KeyExtent(tac.getExtent()).getTableId());
  }

  @Override
  public TabletId getTablet() {
    return new TabletIdImpl(new KeyExtent(tac.getExtent()));
  }

  @Override
  public long getAge() {
    return tac.getAge();
  }

  @Override
  public List<String> getInputFiles() {
    return tac.getInputFiles();
  }

  @Override
  public String getOutputFile() {
    return tac.getOutputFile();
  }

  @Override
  public CompactionType getType() {
    return CompactionType.valueOf(tac.getType().name());
  }

  @Override
  public CompactionReason getReason() {
    return CompactionReason.valueOf(tac.getReason().name());
  }

  @Override
  public String getLocalityGroup() {
    return tac.getLocalityGroup();
  }

  @Override
  public long getEntriesRead() {
    return tac.getEntriesRead();
  }

  @Override
  public long getEntriesWritten() {
    return tac.getEntriesWritten();
  }

  @Override
  public List<IteratorSetting> getIterators() {
    ArrayList<IteratorSetting> ret = new ArrayList<>();

    for (IterInfo ii : tac.getSsiList()) {
      IteratorSetting settings = new IteratorSetting(ii.getPriority(), ii.getIterName(), ii.getClassName());
      Map<String,String> options = tac.getSsio().get(ii.getIterName());
      settings.addOptions(options);

      ret.add(settings);
    }

    return ret;
  }
}
