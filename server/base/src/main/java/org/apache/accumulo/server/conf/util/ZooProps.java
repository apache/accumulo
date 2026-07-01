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
package org.apache.accumulo.server.conf.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.server.conf.store.impl.ReadyMonitor;
import org.apache.accumulo.server.conf.util.ZooInfoViewer.ViewerOpts;
import org.apache.accumulo.server.conf.util.ZooPropEditor.EditorOpts;
import org.apache.accumulo.server.util.ServerKeywordExecutable;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;
import org.apache.accumulo.start.spi.KeywordExecutable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class ZooProps extends ServerKeywordExecutable<ZooProps.PropsOpts> {

  public ZooProps() {
    super(new PropsOpts());
  }

  @Override
  public String keyword() {
    return "props";
  }

  @Override
  public String description() {
    return "Inspect and edit ZooKeeper-backed Accumulo properties. "
        + "Default prints properties for the resolved scope (ZooPropEditor output). "
        + "Use --get for a multi-scope report with --system/--namespaces/--tables filters "
        + "(ZooInfoViewer output). Use --set or --delete to mutate.";
  }

  @Override
  public CommandGroup commandGroup() {
    return CommandGroups.ZOOKEEPER;
  }

  @Override
  public void execute(JCommander cl, PropsOpts opts) throws Exception {

    if (!opts.setOpt.isEmpty() && !opts.deleteOpt.isEmpty()) {
      throw new IllegalArgumentException("Cannot use --set and --delete together.");
    }

    if (!opts.setOpt.isEmpty()) {
      ZooPropEditor editor = buildEditor();
      var context = getServerContext();
      EditorOpts editorOpts = toEditorOpts(opts);
      editorOpts.setOpt = opts.setOpt;
      editor.setProperty(context, editor.getPropKey(context, editorOpts), editorOpts);

    } else if (!opts.deleteOpt.isEmpty()) {
      ZooPropEditor editor = buildEditor();
      var context = getServerContext();
      var zrw = context.getZooSession().asReaderWriter();
      EditorOpts editorOpts = toEditorOpts(opts);
      editorOpts.deleteOpt = opts.deleteOpt;
      var propKey = editor.getPropKey(context, editorOpts);
      editor.deleteProperty(context, propKey, editor.readPropNode(propKey, zrw), editorOpts);

    } else if (opts.getOpt) {
      ZooInfoViewer viewer = buildViewer();
      ViewerOpts viewerOpts = toViewerOpts(opts);
      viewerOpts.printProps = true;

      // TODO
      // Add PR #6419 Json hook

      viewer.generateReport(getServerContext(), viewerOpts);

    } else {
      ZooPropEditor editor = buildEditor();
      var context = getServerContext();
      var zrw = context.getZooSession().asReaderWriter();
      EditorOpts editorOpts = toEditorOpts(opts);
      var propKey = editor.getPropKey(context, editorOpts);
      editor.printProperties(context, propKey, editor.readPropNode(propKey, zrw));

      // TODO
      // Add PR #6419 Json hook

    }
  }

  private ZooPropEditor buildEditor() {
    ZooPropEditor editor = new ZooPropEditor();
    editor.nullWatcher = new ZooPropEditor.NullWatcher(
        new ReadyMonitor(ZooPropEditor.class.getSimpleName(), 20_000L));
    return editor;
  }

  private ZooInfoViewer buildViewer() {
    ZooInfoViewer viewer = new ZooInfoViewer();
    viewer.nullWatcher = new ZooInfoViewer.NullWatcher(
        new ReadyMonitor(ZooInfoViewer.class.getSimpleName(), 20_000L));
    return viewer;
  }

  private EditorOpts toEditorOpts(PropsOpts opts) {
    EditorOpts e = new EditorOpts();
    e.tableOpt = opts.tableOpt;
    e.tableIdOpt = opts.tableIdOpt;
    e.namespaceOpt = opts.namespaceOpt;
    e.namespaceIdOpt = opts.namespaceIdOpt;
    e.resourceGroupOpt = opts.resourceGroupOpt.isEmpty() ? "" : opts.resourceGroupOpt.get(0);
    return e;
  }

  private ViewerOpts toViewerOpts(PropsOpts opts) {
    ViewerOpts v = new ViewerOpts();
    v.outfile = opts.outfile;
    v.printSystemOpt = opts.systemOpt;
    v.tablesOpt.addAll(opts.tablesOpt);
    v.namespacesOpt.addAll(opts.namespacesOpt);
    v.resourceGroupOpt.addAll(opts.resourceGroupOpt);
    return v;
  }

  static class PropsOpts extends ServerOpts {
    @Parameter(names = {"-s", "--set"}, description = "set a property")
    String setOpt = "";

    @Parameter(names = {"-d", "--delete"}, description = "delete a property")
    String deleteOpt = "";

    @Parameter(names = {"-t", "--table"},
        description = "table to display/set/delete properties for")
    String tableOpt = "";

    @Parameter(names = {"-tid", "--table-id"},
        description = "table id to display/set/delete properties for")
    String tableIdOpt = "";

    @Parameter(names = {"-ns", "--namespace"},
        description = "namespace to display/set/delete properties for")
    String namespaceOpt = "";

    @Parameter(names = {"-nid", "--namespace-id"},
        description = "namespace id to display/set/delete properties for")
    String namespaceIdOpt = "";

    @Parameter(names = {"-r", "--resource-group"},
        description = "resource group name to display/set/delete properties for",
        variableArity = true)
    List<String> resourceGroupOpt = new ArrayList<>();

    @Parameter(names = {"--get"},
        description = "print a multi-scope property report (ZooInfoViewer output). "
            + "Enables --system, --namespaces, and --tables filter flags.")
    boolean getOpt = false;

    @Parameter(names = {"--system"},
        description = "print the properties for the system config. Only valid with --get")
    boolean systemOpt = false;

    @Parameter(names = {"--namespaces"},
        description = "a list of namespace names to print properties, with none specified, print all. Only valid with --get",
        variableArity = true)
    List<String> namespacesOpt = new ArrayList<>();

    @Parameter(names = {"--tables"},
        description = "a list of table names to print properties. Only valid with --get",
        variableArity = true)
    List<String> tablesOpt = new ArrayList<>();

    @Parameter(names = {"--outfile"},
        description = "Write the output to a file, if the file exists will not be overwritten.")
    String outfile = "";

    @Override
    public void parseArgs(String programName, String[] args, Object... others) {
      super.parseArgs(programName, args, others);
      if (!setOpt.isEmpty() && !deleteOpt.isEmpty()) {
        throw new IllegalArgumentException("Cannot use --set and --delete in one command");
      }
      if ((!systemOpt || !namespacesOpt.isEmpty() || !tablesOpt.isEmpty()) && !getOpt
          && setOpt.isEmpty() && deleteOpt.isEmpty()) {
        if (!namespacesOpt.isEmpty() || !tablesOpt.isEmpty()) {
          throw new IllegalArgumentException("--namespaces and --tables are only valid with --get");
        }
      }
    }
  }
}
