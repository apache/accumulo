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
package org.apache.accumulo.manager.split;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataOperations;
import org.apache.accumulo.core.metadata.OperationId;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.FileUtil;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitTask implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(SplitTask.class);

  private ServerContext context;
  private Set<KeyExtent> splitting;
  private TabletMetadata tablet;

  public SplitTask(ServerContext context, Set<KeyExtent> splitting, TabletMetadata tablet) {
    this.context = context;
    this.splitting = splitting;
    this.tablet = tablet;
  }

  @Override
  public void run() {
    try {
      System.out.println("running split task for " + tablet.getExtent());

      var extent = tablet.getExtent();
      var files =
          tablet.getFiles().stream().map(TabletFile::getPathStr).collect(Collectors.toList());
      var tableConfiguration = context.getTableConfiguration(extent.tableId());

      SortedMap<Double,Key> keys = FileUtil.findMidPoint(context, tableConfiguration, null,
          extent.prevEndRow(), extent.endRow(), files, .25, true);

      // TODO code in tablet class does alot of checks that need to be done here.
      Text split = keys.get(.5).getRow();
      var splits = new TreeSet<Text>();
      splits.add(split);

      System.out.println("splitting " + tablet.getExtent() + " at " + split);
      MetadataOperations.doSplit(context.getAmple(), extent, splits,
          new OperationId(UUID.randomUUID().toString()));

    } catch (Exception e) {
      log.error("Failed to split {}", tablet.getExtent(), e);
    } finally {
      splitting.remove(tablet.getExtent());
    }

  }
}
