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
package org.apache.accumulo.examples.simple.dirlist;

import java.awt.BorderLayout;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Map;
import java.util.Map.Entry;

import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.JTree;
import javax.swing.event.TreeExpansionEvent;
import javax.swing.event.TreeExpansionListener;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.examples.simple.filedata.FileDataQuery;
import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;

/**
 * Provides a GUI for browsing the file system information stored in Accumulo. See docs/examples/README.dirlist for instructions.
 */
@SuppressWarnings("serial")
public class Viewer extends JFrame implements TreeSelectionListener, TreeExpansionListener {
  private static final Logger log = Logger.getLogger(Viewer.class);

  JTree tree;
  DefaultTreeModel treeModel;
  QueryUtil q;
  FileDataQuery fdq;
  String topPath;
  Map<String,DefaultMutableTreeNode> nodeNameMap;
  JTextArea text;
  JTextArea data;
  JScrollPane dataPane;

  public static class NodeInfo {
    private String name;
    private Map<String,String> data;

    public NodeInfo(String name, Map<String,String> data) {
      this.name = name;
      this.data = data;
    }

    public String getName() {
      return name;
    }

    public String getFullName() {
      String fn = data.get("fullname");
      if (fn == null)
        return name;
      return fn;
    }

    public Map<String,String> getData() {
      return data;
    }

    public String toString() {
      return getName();
    }

    public String getHash() {
      for (String k : data.keySet()) {
        String[] parts = k.split(":");
        if (parts.length >= 2 && parts[1].equals("md5")) {
          return data.get(k);
        }
      }
      return null;
    }
  }

  public Viewer(Opts opts) throws Exception {
    super("File Viewer");
    setSize(1000, 800);
    setDefaultCloseOperation(EXIT_ON_CLOSE);
    q = new QueryUtil(opts);
    fdq = new FileDataQuery(opts.instance, opts.zookeepers, opts.principal, opts.getToken(), opts.dataTable, opts.auths);
    this.topPath = opts.path;
  }

  public void populate(DefaultMutableTreeNode node) throws TableNotFoundException {
    String path = ((NodeInfo) node.getUserObject()).getFullName();
    log.debug("listing " + path);
    for (Entry<String,Map<String,String>> e : q.getDirList(path).entrySet()) {
      log.debug("got child for " + node.getUserObject() + ": " + e.getKey());
      node.add(new DefaultMutableTreeNode(new NodeInfo(e.getKey(), e.getValue())));
    }
  }

  public void populateChildren(DefaultMutableTreeNode node) throws TableNotFoundException {
    @SuppressWarnings("unchecked")
    Enumeration<DefaultMutableTreeNode> children = node.children();
    while (children.hasMoreElements()) {
      populate(children.nextElement());
    }
  }

  public void init() throws TableNotFoundException {
    DefaultMutableTreeNode root = new DefaultMutableTreeNode(new NodeInfo(topPath, q.getData(topPath)));
    populate(root);
    populateChildren(root);

    treeModel = new DefaultTreeModel(root);
    tree = new JTree(treeModel);
    tree.addTreeExpansionListener(this);
    tree.addTreeSelectionListener(this);
    text = new JTextArea(getText(q.getData(topPath)));
    data = new JTextArea("");
    JScrollPane treePane = new JScrollPane(tree);
    JScrollPane textPane = new JScrollPane(text);
    dataPane = new JScrollPane(data);
    JSplitPane infoSplitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, textPane, dataPane);
    JSplitPane mainSplitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, treePane, infoSplitPane);
    mainSplitPane.setDividerLocation(300);
    infoSplitPane.setDividerLocation(150);
    getContentPane().add(mainSplitPane, BorderLayout.CENTER);
  }

  public static String getText(DefaultMutableTreeNode node) {
    return getText(((NodeInfo) node.getUserObject()).getData());
  }

  public static String getText(Map<String,String> data) {
    StringBuilder sb = new StringBuilder();
    for (String name : data.keySet()) {
      sb.append(name);
      sb.append(" : ");
      sb.append(data.get(name));
      sb.append('\n');
    }
    return sb.toString();
  }

  @Override
  public void treeExpanded(TreeExpansionEvent event) {
    try {
      populateChildren((DefaultMutableTreeNode) event.getPath().getLastPathComponent());
    } catch (TableNotFoundException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void treeCollapsed(TreeExpansionEvent event) {
    DefaultMutableTreeNode node = (DefaultMutableTreeNode) event.getPath().getLastPathComponent();
    @SuppressWarnings("unchecked")
    Enumeration<DefaultMutableTreeNode> children = node.children();
    while (children.hasMoreElements()) {
      DefaultMutableTreeNode child = children.nextElement();
      log.debug("removing children of " + ((NodeInfo) child.getUserObject()).getFullName());
      child.removeAllChildren();
    }
  }

  @Override
  public void valueChanged(TreeSelectionEvent e) {
    TreePath selected = e.getNewLeadSelectionPath();
    if (selected == null)
      return;
    DefaultMutableTreeNode node = (DefaultMutableTreeNode) selected.getLastPathComponent();
    text.setText(getText(node));
    try {
      String hash = ((NodeInfo) node.getUserObject()).getHash();
      if (hash != null) {
        data.setText(fdq.getSomeData(hash, 10000));
      } else {
        data.setText("");
      }
    } catch (IOException e1) {
      e1.printStackTrace();
    }
  }

  static class Opts extends QueryUtil.Opts {
    @Parameter(names = "--dataTable")
    String dataTable = "dataTable";
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(Viewer.class.getName(), args);

    Viewer v = new Viewer(opts);
    v.init();
    v.setVisible(true);
  }
}
