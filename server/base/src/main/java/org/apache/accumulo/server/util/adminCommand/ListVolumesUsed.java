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
package org.apache.accumulo.server.util.adminCommand;

import java.util.Iterator;
import java.util.TreeSet;

import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.gc.GcCandidate;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.util.ServerKeywordExecutable;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.JCommander;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class ListVolumesUsed extends ServerKeywordExecutable<ServerOpts> {

  public ListVolumesUsed() {
    super(new ServerOpts());
  }

  @Override
  public String keyword() {
    return "list-volumes";
  }

  @Override
  public CommandGroup commandGroup() {
    return CommandGroups.ADMIN;
  }

  @Override
  public String description() {
    return "list volumes currently in use";
  }

  @Override
  public void execute(JCommander cl, ServerOpts options) throws Exception {
    ServerContext context = getServerContext();
    listTable(Ample.DataLevel.ROOT, context);
    System.out.println();
    listTable(Ample.DataLevel.METADATA, context);
    System.out.println();
    listTable(Ample.DataLevel.USER, context);
  }

  private String getTableURI(String rootTabletDir) {
    Path ret = FileType.TABLE.getVolume(new Path(rootTabletDir));
    if (ret == null) {
      return "RELATIVE";
    }
    return ret.toString();
  }

  private String getLogURI(String logEntry) {
    Path ret = FileType.WAL.getVolume(new Path(logEntry));
    if (ret == null) {
      return "RELATIVE";
    }
    return ret.toString();
  }

  private void getLogURIs(TreeSet<String> volumes, LogEntry logEntry) {
    volumes.add(getLogURI(logEntry.getPath()));
  }

  private void listTable(Ample.DataLevel level, ServerContext context) throws Exception {

    System.out.println("Listing volumes referenced in " + level + " tablets section");

    TreeSet<String> volumes = new TreeSet<>();
    try (TabletsMetadata tablets = TabletsMetadata.builder(context).forLevel(level)
        .fetch(TabletMetadata.ColumnType.FILES, TabletMetadata.ColumnType.LOGS).build()) {
      for (TabletMetadata tabletMetadata : tablets) {
        tabletMetadata.getFiles()
            .forEach(file -> volumes.add(getTableURI(file.getNormalizedPathStr())));
        tabletMetadata.getLogs().forEach(le -> getLogURIs(volumes, le));
      }
    }

    for (String volume : volumes) {
      System.out.println("\tVolume : " + volume);
    }

    System.out.println("Listing volumes referenced in " + level
        + " deletes section (volume replacement occurs at deletion time)");
    volumes.clear();

    Iterator<GcCandidate> delPaths = context.getAmple().getGcCandidates(level);
    while (delPaths.hasNext()) {
      volumes.add(getTableURI(delPaths.next().getPath()));
    }
    for (String volume : volumes) {
      System.out.println("\tVolume : " + volume);
    }

    System.out.println("Listing volumes referenced in " + level + " current logs");
    volumes.clear();

    WalStateManager wals = new WalStateManager(context);
    for (Path path : wals.getAllState().keySet()) {
      volumes.add(getLogURI(path.toString()));
    }
    for (String volume : volumes) {
      System.out.println("\tVolume : " + volume);
    }
  }

}
