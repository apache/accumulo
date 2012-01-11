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
package org.apache.accumulo.examples.wikisearch.parser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Vector;

import org.apache.accumulo.examples.wikisearch.parser.QueryParser.QueryTerm;
import org.apache.commons.jexl2.parser.JexlNode;


import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class TreeNode {
  
  private Class<? extends JexlNode> type = null;
  /* navigation elements */
  private TreeNode parent = null;
  private List<TreeNode> children = new ArrayList<TreeNode>();
  private Multimap<String,QueryTerm> terms = HashMultimap.create();
  
  public TreeNode() {
    super();
  }
  
  public Class<? extends JexlNode> getType() {
    return type;
  }
  
  public TreeNode getParent() {
    return parent;
  }
  
  public List<TreeNode> getChildren() {
    return children;
  }
  
  public Enumeration<TreeNode> getChildrenAsEnumeration() {
    return Collections.enumeration(children);
  }
  
  public Multimap<String,QueryTerm> getTerms() {
    return terms;
  }
  
  public void setType(Class<? extends JexlNode> type) {
    this.type = type;
  }
  
  public void setParent(TreeNode parent) {
    this.parent = parent;
  }
  
  public void setChildren(List<TreeNode> children) {
    this.children = children;
  }
  
  public void setTerms(Multimap<String,QueryTerm> terms) {
    this.terms = terms;
  }
  
  public boolean isLeaf() {
    return children.isEmpty();
  }
  
  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("Type: ").append(type.getSimpleName());
    buf.append(" Terms: ");
    if (null == terms) {
      buf.append("null");
    } else {
      buf.append(terms.toString());
    }
    return buf.toString();
  }
  
  public final Enumeration<?> depthFirstEnumeration() {
    return new PostorderEnumeration(this);
  }
  
  public Enumeration<?> breadthFirstEnumeration() {
    return new BreadthFirstEnumeration(this);
  }
  
  public final class PostorderEnumeration implements Enumeration<TreeNode> {
    
    protected TreeNode root;
    protected Enumeration<TreeNode> children;
    protected Enumeration<TreeNode> subtree;
    
    public PostorderEnumeration(TreeNode rootNode) {
      super();
      root = rootNode;
      children = root.getChildrenAsEnumeration();
      subtree = EMPTY_ENUMERATION;
    }
    
    public boolean hasMoreElements() {
      return root != null;
    }
    
    public TreeNode nextElement() {
      TreeNode retval;
      
      if (subtree.hasMoreElements()) {
        retval = subtree.nextElement();
      } else if (children.hasMoreElements()) {
        subtree = new PostorderEnumeration((TreeNode) children.nextElement());
        retval = subtree.nextElement();
      } else {
        retval = root;
        root = null;
      }
      
      return retval;
    }
  } // End of class PostorderEnumeration
  
  static public final Enumeration<TreeNode> EMPTY_ENUMERATION = new Enumeration<TreeNode>() {
    
    public boolean hasMoreElements() {
      return false;
    }
    
    public TreeNode nextElement() {
      throw new NoSuchElementException("No more elements");
    }
  };
  
  final class BreadthFirstEnumeration implements Enumeration<TreeNode> {
    protected Queue queue;
    
    public BreadthFirstEnumeration(TreeNode rootNode) {
      super();
      Vector<TreeNode> v = new Vector<TreeNode>(1);
      v.addElement(rootNode); // PENDING: don't really need a vector
      queue = new Queue();
      queue.enqueue(v.elements());
    }
    
    public boolean hasMoreElements() {
      return (!queue.isEmpty() && ((Enumeration<?>) queue.firstObject()).hasMoreElements());
    }
    
    public TreeNode nextElement() {
      Enumeration<?> enumer = (Enumeration<?>) queue.firstObject();
      TreeNode node = (TreeNode) enumer.nextElement();
      Enumeration<?> children = node.getChildrenAsEnumeration();
      
      if (!enumer.hasMoreElements()) {
        queue.dequeue();
      }
      if (children.hasMoreElements()) {
        queue.enqueue(children);
      }
      return node;
    }
    
    // A simple queue with a linked list data structure.
    final class Queue {
      QNode head; // null if empty
      QNode tail;
      
      final class QNode {
        public Object object;
        public QNode next; // null if end
        
        public QNode(Object object, QNode next) {
          this.object = object;
          this.next = next;
        }
      }
      
      public void enqueue(Object anObject) {
        if (head == null) {
          head = tail = new QNode(anObject, null);
        } else {
          tail.next = new QNode(anObject, null);
          tail = tail.next;
        }
      }
      
      public Object dequeue() {
        if (head == null) {
          throw new NoSuchElementException("No more elements");
        }
        
        Object retval = head.object;
        QNode oldHead = head;
        head = head.next;
        if (head == null) {
          tail = null;
        } else {
          oldHead.next = null;
        }
        return retval;
      }
      
      public Object firstObject() {
        if (head == null) {
          throw new NoSuchElementException("No more elements");
        }
        
        return head.object;
      }
      
      public boolean isEmpty() {
        return head == null;
      }
      
    } // End of class Queue
    
  } // End of class BreadthFirstEnumeration
}
