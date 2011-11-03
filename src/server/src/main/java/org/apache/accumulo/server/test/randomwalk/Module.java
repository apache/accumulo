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
package org.apache.accumulo.server.test.randomwalk;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * A module is directed graph of tests
 */
public class Module extends Node {
  
  private class Dummy extends Node {
    
    String name;
    
    Dummy(String name) {
      this.name = name;
    }
    
    @Override
    public void visit(State state, Properties props) {
      throw new UnsupportedOperationException();
    }
    
    public String toString() {
      return name;
    }
  }
  
  private HashMap<String,Node> nodes = new HashMap<String,Node>();
  private HashMap<String,Properties> localProps = new HashMap<String,Properties>();
  
  private class Edge {
    String nodeId;
    int weight;
  }
  
  private class AdjList {
    
    private List<Edge> edges = new ArrayList<Edge>();
    private int totalWeight = 0;
    private Random rand = new Random();
    
    /**
     * Adds a neighbor node and weight of edge
     */
    private void addEdge(String nodeId, int weight) {
      
      totalWeight += weight;
      
      Edge e = new Edge();
      e.nodeId = nodeId;
      e.weight = weight;
      edges.add(e);
    }
    
    /**
     * Chooses a random neighbor node
     * 
     * @return Node or null if no edges
     * @throws Exception
     */
    private String randomNeighbor() throws Exception {
      
      String nodeId = null;
      rand = new Random();
      
      int randNum = rand.nextInt(totalWeight) + 1;
      int sum = 0;
      
      for (Edge e : edges) {
        nodeId = e.nodeId;
        sum += e.weight;
        if (randNum <= sum) {
          break;
        }
      }
      return nodeId;
    }
  }
  
  private HashMap<String,String> prefixes = new HashMap<String,String>();
  private HashMap<String,AdjList> adjMap = new HashMap<String,AdjList>();
  private final File xmlFile;
  private String initNodeId;
  private Fixture fixture = null;
  
  public Module(File xmlFile) throws Exception {
    this.xmlFile = xmlFile;
    loadFromXml();
  }
  
  @Override
  public void visit(State state, Properties props) throws Exception {
    
    int maxHops = Integer.parseInt(props.getProperty("maxHops", "0"));
    if (maxHops == 0) { // zero indicates run forever
      maxHops = Integer.MAX_VALUE;
    }
    int maxSec = Integer.parseInt(props.getProperty("maxSec", "0"));
    if (maxSec == 0) { // zero indicated run forever
      maxSec = Integer.MAX_VALUE;
    }
    boolean teardown = Boolean.parseBoolean(props.getProperty("teardown", "true"));
    
    if (fixture != null) {
      fixture.setUp(state);
    }
    
    Node initNode = getNode(initNodeId);
    if ((initNode instanceof Dummy) == false) {
      initNode.visit(state, getProps(initNodeId));
      state.visitedNode();
    }
    
    String curNodeId = initNodeId;
    int numHops = 0;
    long startTime = System.currentTimeMillis() / 1000;
    while (true) {
      
      // check if END state was reached
      if (curNodeId.equalsIgnoreCase("END")) {
        log.debug("reached END state");
        break;
      }
      // check if maxSec was reached
      long curTime = System.currentTimeMillis() / 1000;
      if ((curTime - startTime) > maxSec) {
        log.debug("reached maxSec(" + maxSec + ")");
        break;
      }
      // check if maxHops was reached
      if (numHops > maxHops) {
        log.debug("reached maxHops(" + maxHops + ")");
        break;
      }
      numHops++;
      
      if (!adjMap.containsKey(curNodeId)) {
        throw new Exception("Reached node(" + curNodeId + ") without outgoing edges in module(" + this + ")");
      }
      AdjList adj = adjMap.get(curNodeId);
      String nextNodeId = adj.randomNeighbor();
      Node nextNode = getNode(nextNodeId);
      if ((nextNode instanceof Dummy) == false) {
        nextNode.visit(state, getProps(nextNodeId));
        state.visitedNode();
      }
      curNodeId = nextNodeId;
    }
    
    if (teardown && (fixture != null)) {
      log.debug("tearing down module");
      fixture.tearDown(state);
    }
  }
  
  @Override
  public String toString() {
    return xmlFile.toString();
  }
  
  private String getFullName(String name) {
    
    int index = name.indexOf(".");
    if ((index == -1) || name.endsWith(".xml")) {
      return name;
    }
    
    String id = name.substring(0, index);
    
    if (!prefixes.containsKey(id)) {
      log.warn("Id (" + id + ") was not found in prefixes");
      return name;
    }
    
    return prefixes.get(id).concat(name.substring(index + 1));
  }
  
  private Node createNode(String id, String src) throws Exception {
    
    // check if id indicates dummy node
    if (id.equalsIgnoreCase("END") || id.startsWith("dummy")) {
      if (nodes.containsKey(id) == false) {
        nodes.put(id, new Dummy(id));
      }
      return nodes.get(id);
    }
    
    // grab node from framework based on its id or src
    Node node;
    if (src == null || src.isEmpty()) {
      node = Framework.getInstance().getNode(getFullName(id));
    } else {
      node = Framework.getInstance().getNode(getFullName(src));
    }
    
    // add to node to this module's map
    nodes.put(id, node);
    
    return node;
  }
  
  private Node getNode(String id) throws Exception {
    
    if (nodes.containsKey(id)) {
      return nodes.get(id);
    }
    
    if (id.equalsIgnoreCase("END")) {
      nodes.put(id, new Dummy(id));
      return nodes.get(id);
    }
    
    return Framework.getInstance().getNode(getFullName(id));
  }
  
  private Properties getProps(String nodeId) {
    if (localProps.containsKey(nodeId)) {
      return localProps.get(nodeId);
    }
    return new Properties();
  }
  
  private void loadFromXml() throws Exception {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder docbuilder;
    Document d = null;
    
    // set the schema
    SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema moduleSchema = sf.newSchema(this.getClass().getClassLoader().getResource("randomwalk/module.xsd"));
    dbf.setSchema(moduleSchema);
    
    // parse the document
    try {
      docbuilder = dbf.newDocumentBuilder();
      d = docbuilder.parse(xmlFile);
    } catch (Exception e) {
      log.error("Failed to parse: " + xmlFile, e);
      throw new Exception("Failed to parse: " + xmlFile);
    }
    
    // parse packages
    NodeList nodelist = d.getDocumentElement().getElementsByTagName("package");
    for (int i = 0; i < nodelist.getLength(); i++) {
      Element el = (Element) nodelist.item(i);
      String value = el.getAttribute("value");
      if (!value.endsWith(".")) {
        value = value.concat(".");
      }
      prefixes.put(el.getAttribute("prefix"), value);
    }
    
    // parse fixture node
    nodelist = d.getDocumentElement().getElementsByTagName("fixture");
    if (nodelist.getLength() > 0) {
      Element fixtureEl = (Element) nodelist.item(0);
      fixture = (Fixture) Class.forName(getFullName(fixtureEl.getAttribute("id"))).newInstance();
    }
    
    // parse initial node
    Element initEl = (Element) d.getDocumentElement().getElementsByTagName("init").item(0);
    initNodeId = initEl.getAttribute("id");
    
    // parse all nodes
    nodelist = d.getDocumentElement().getElementsByTagName("node");
    for (int i = 0; i < nodelist.getLength(); i++) {
      
      Element nodeEl = (Element) nodelist.item(i);
      
      // get attributes
      String id = nodeEl.getAttribute("id");
      if (adjMap.containsKey(id)) {
        throw new Exception("Module already contains: " + id);
      }
      String src = nodeEl.getAttribute("src");
      
      // create node
      createNode(id, src);
      
      // set some attributes in properties for later use
      Properties props = new Properties();
      props.setProperty("maxHops", nodeEl.getAttribute("maxHops"));
      props.setProperty("maxSec", nodeEl.getAttribute("maxSec"));
      props.setProperty("teardown", nodeEl.getAttribute("maxHops"));
      
      // parse properties of nodes
      NodeList proplist = nodeEl.getElementsByTagName("property");
      for (int j = 0; j < proplist.getLength(); j++) {
        Element propEl = (Element) proplist.item(j);
        
        if (!propEl.hasAttribute("key") || !propEl.hasAttribute("value")) {
          throw new Exception("Node " + id + " has property with no key or value");
        }
        
        String key = propEl.getAttribute("key");
        
        if (key.equals("maxHops") || key.equals("maxSec") || key.equals("teardown")) {
          throw new Exception("The following property can only be set in attributes: " + key);
        }
        
        props.setProperty(key, propEl.getAttribute("value"));
      }
      localProps.put(id, props);
      
      // parse edges of nodes
      AdjList edges = new AdjList();
      adjMap.put(id, edges);
      NodeList edgelist = nodeEl.getElementsByTagName("edge");
      if (edgelist.getLength() == 0) {
        throw new Exception("Node " + id + " has no edges!");
      }
      for (int j = 0; j < edgelist.getLength(); j++) {
        Element edgeEl = (Element) edgelist.item(j);
        
        String edgeID = edgeEl.getAttribute("id");
        
        if (!edgeEl.hasAttribute("weight")) {
          throw new Exception("Edge with id=" + edgeID + " is missing weight");
        }
        
        int weight = Integer.parseInt(edgeEl.getAttribute("weight"));
        edges.addEdge(edgeID, weight);
      }
    }
  }
}
