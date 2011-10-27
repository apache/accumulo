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
package org.apache.accumulo.examples.dirlist;

import java.awt.BorderLayout;
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
import org.apache.accumulo.core.security.Authorizations;

@SuppressWarnings("serial")
public class Viewer extends JFrame implements TreeSelectionListener, TreeExpansionListener {
  JTree tree;
  DefaultTreeModel treeModel;
  QueryUtil q;
  String topPath;
  Map<String,DefaultMutableTreeNode> nodeNameMap;
  JTextArea text;
  
  public static class NodeInfo {
    private String name;
    private Map<String,String> data;
    private boolean lookedUpChildren;
    
    public NodeInfo(String name, Map<String,String> data) {
      this.name = name;
      this.data = data;
      this.lookedUpChildren = false;
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
    
    public boolean haveLookedUpChildren() {
      return lookedUpChildren;
    }
    
    public void setLookedUpChildren(boolean lookedUpChildren) {
      this.lookedUpChildren = lookedUpChildren;
    }
  }
  
  public Viewer(String instanceName, String zooKeepers, String user, String password, String tableName, Authorizations auths, String path) throws Exception {
    super("File Viewer");
    setSize(800, 800);
    setDefaultCloseOperation(EXIT_ON_CLOSE);
    q = new QueryUtil(instanceName, zooKeepers, user, password, tableName, auths);
    this.topPath = path;
  }
  
  public void populate(DefaultMutableTreeNode node) throws TableNotFoundException {
    String path = ((NodeInfo) node.getUserObject()).getFullName();
    System.out.println("listing " + path);
    for (Entry<String,Map<String,String>> e : q.getDirList(path).entrySet()) {
      System.out.println("got child for " + node.getUserObject() + ": " + e.getKey());
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
    JScrollPane treePane = new JScrollPane(tree);
    JScrollPane textPane = new JScrollPane(text);
    JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, treePane, textPane);
    splitPane.setDividerLocation(0.5);
    getContentPane().add(splitPane, BorderLayout.CENTER);
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
      // TODO Auto-generated catch block
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
      System.out.println("removing children of " + ((NodeInfo) child.getUserObject()).getFullName());
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
  }
  
  public static void main(String[] args) throws Exception {
    if (args.length != 7 && args.length != 6) {
      System.out.println("usage: " + Viewer.class.getSimpleName() + " <instance> <zoo> <user> <pass> <table> <auths> [rootpath]");
      System.exit(1);
    }
    String rootpath = "/";
    if (args.length == 7)
      rootpath = args[6];
    Viewer v = new Viewer(args[0], args[1], args[2], args[3], args[4], new Authorizations(args[5].split(",")), rootpath);
    v.init();
    v.setVisible(true);
  }
}
