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
package org.apache.accumulo.examples.wikisearch.iterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeNode;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.examples.wikisearch.parser.JexlOperatorConstants;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;


/**
 *
 * 
 */
public class BooleanLogicTreeNode extends DefaultMutableTreeNode {
  
  private static final long serialVersionUID = 1L;
  protected static final Logger log = Logger.getLogger(BooleanLogicTreeNode.class);
  private Key myTopKey = null;
  private Key advanceKey = null;
  private Text fValue = null;
  private Text fName = null;
  private boolean negated = false;
  private int type;
  private boolean done = false;
  private boolean valid = false;
  private boolean rollUp = false;
  private String fOperator = null;
  private boolean childrenAllNegated = false;
  private HashSet<Key> uids;
  private Text upperBound;
  private Text lowerBound;
  private boolean rangeNode;
  
  public BooleanLogicTreeNode() {
    super();
    uids = new HashSet<Key>();
  }
  
  public BooleanLogicTreeNode(int type) {
    super();
    this.type = type;
    uids = new HashSet<Key>();
    setOperator();
  }
  
  public BooleanLogicTreeNode(int type, boolean negate) {
    super();
    this.type = type;
    this.negated = negate;
    uids = new HashSet<Key>();
    setOperator();
  }
  
  public BooleanLogicTreeNode(int type, String fieldName, String fieldValue) {
    super();
    this.type = type;
    if (fieldValue != null) {
      this.fValue = new Text(fieldValue);
    }
    if (fieldName != null) {
      this.fName = new Text(fieldName);
    }
    uids = new HashSet<Key>();
    setOperator();
  }
  
  public BooleanLogicTreeNode(int type, String fieldName, String fieldValue, boolean negated) {
    super();
    this.type = type;
    if (fieldValue != null) {
      this.fValue = new Text(fieldValue);
    }
    if (fieldName != null) {
      this.fName = new Text(fieldName);
    }
    uids = new HashSet<Key>();
    this.negated = negated;
    setOperator();
  }
  
  public void setValid(boolean b) {
    this.valid = b;
  }
  
  public boolean isValid() {
    return this.valid;
  }
  
  public void setType(int t) {
    this.type = t;
  }
  
  public int getType() {
    return this.type;
  }
  
  public void setChildrenAllNegated(boolean childrenAllNegated) {
    this.childrenAllNegated = childrenAllNegated;
  }
  
  public boolean isChildrenAllNegated() {
    return childrenAllNegated;
  }
  
  public void setAdvanceKey(Key advanceKey) {
    this.advanceKey = advanceKey;
  }
  
  public Key getAdvanceKey() {
    return advanceKey;
  }
  
  public void setNegated(boolean b) {
    this.negated = b;
  }
  
  public boolean isNegated() {
    return negated;
  }
  
  public void setTopKey(Key id) {
    this.myTopKey = id;
  }
  
  public Key getTopKey() {
    return myTopKey;
  }
  
  public void setDone(boolean done) {
    this.done = done;
  }
  
  public boolean isDone() {
    return done;
  }
  
  public void setRollUp(boolean rollUp) {
    this.rollUp = rollUp;
  }
  
  public boolean isRollUp() {
    return rollUp;
  }
  
  public Text getFieldValue() {
    return fValue;
  }
  
  public void setFieldValue(Text term) {
    this.fValue = term;
  }
  
  public Text getFieldName() {
    return fName;
  }
  
  public void setFieldName(Text dataLocation) {
    this.fName = dataLocation;
  }
  
  public String getFieldOperator() {
    return fOperator;
  }
  
  private void setOperator() {
    this.fOperator = JexlOperatorConstants.getOperator(type);
    if (negated && this.fOperator.equals("!=")) {
      this.fOperator = JexlOperatorConstants.getOperator(JexlOperatorConstants.JJTEQNODE);
    }
  }
  
  public Text getLowerBound() {
    return lowerBound;
  }
  
  public void setLowerBound(Text lowerBound) {
    this.lowerBound = lowerBound;
  }
  
  public Text getUpperBound() {
    return upperBound;
  }
  
  public void setUpperBound(Text upperBound) {
    this.upperBound = upperBound;
  }
  
  public boolean isRangeNode() {
    return rangeNode;
  }
  
  public void setRangeNode(boolean rangeNode) {
    this.rangeNode = rangeNode;
  }
  
  public String getContents() {
    StringBuilder s = new StringBuilder("[");
    s.append(toString());
    
    if (children != null) {
      Enumeration<?> e = this.children();
      while (e.hasMoreElements()) {
        BooleanLogicTreeNode n = (BooleanLogicTreeNode) e.nextElement();
        s.append(",");
        s.append(n.getContents());
      }
    }
    s.append("]");
    return s.toString();
  }
  
  public String printNode() {
    StringBuilder s = new StringBuilder("[");
    s.append("Full Location & Term = ");
    if (this.fName != null) {
      s.append(this.fName.toString());
    } else {
      s.append("BlankDataLocation");
    }
    s.append("  ");
    if (this.fValue != null) {
      s.append(this.fValue.toString());
    } else {
      s.append("BlankTerm");
    }
    s.append("]");
    return s.toString();
  }
  
  @Override
  public String toString() {
    String uidStr = "none";
    if (myTopKey != null) {
      String cf = myTopKey.getColumnFamily().toString();
      
      uidStr = cf;
    }
    switch (type) {
      case ParserTreeConstants.JJTEQNODE:
        return fName.toString() + ":" + fValue.toString() + ", uid=" + uidStr + " , negation=" + this.isNegated();
      case ParserTreeConstants.JJTNENODE:
        return fName.toString() + ":" + fValue.toString() + ", uid=" + uidStr + " , negation=" + this.isNegated();
      case ParserTreeConstants.JJTERNODE:
        return fName.toString() + ":" + fValue.toString() + ", uid=" + uidStr + " , negation=" + this.isNegated();
      case ParserTreeConstants.JJTNRNODE:
        return fName.toString() + ":" + fValue.toString() + ", uid=" + uidStr + " , negation=" + this.isNegated();
      case ParserTreeConstants.JJTLENODE:
        return "<=:" + fName.toString() + ":" + fValue.toString() + ", uid=" + uidStr + " , negation=" + this.isNegated();
      case ParserTreeConstants.JJTLTNODE:
        return "<:" + fName.toString() + ":" + fValue.toString() + ", uid=" + uidStr + " , negation=" + this.isNegated();
      case ParserTreeConstants.JJTGENODE:
        return ">=:" + fName.toString() + ":" + fValue.toString() + ", uid=" + uidStr + " , negation=" + this.isNegated();
      case ParserTreeConstants.JJTGTNODE:
        return ">:" + fName.toString() + ":" + fValue.toString() + ", uid=" + uidStr + " , negation=" + this.isNegated();
      case ParserTreeConstants.JJTJEXLSCRIPT:
        return "HEAD" + ":" + uidStr + ":" + isValid();
      case ParserTreeConstants.JJTANDNODE:
        return "AND" + ":" + uidStr + ":" + isValid();
      case ParserTreeConstants.JJTNOTNODE:
        return "NOT";
      case ParserTreeConstants.JJTORNODE:
        return "OR" + ":" + uidStr + ":" + isValid();
      default:
        System.out.println("Problem in BLTNODE.toString()");
        return null;
    }
  }
  
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    
    // always start fresh
    this.setTopKey(null);
    this.setDone(false);
    
    // get my user object which should be an iterator
    SortedKeyValueIterator<?,?> iter = (SortedKeyValueIterator<?,?>) this.getUserObject();
    if (iter != null) {
      
      iter.seek(range, columnFamilies, inclusive);
      
      if (iter.hasTop()) {
        Key key = (Key) iter.getTopKey();
        key = buildKey(key);
        
        this.setTopKey(key);
        if (log.isDebugEnabled()) {
          log.debug("BLTNODE.seek() -> found: " + this.getTopKey());
        }
      } else {
        if (log.isDebugEnabled()) {
          log.debug("BLTNODE.seek() -> hasTop::false");
        }
        this.setDone(true);
      }
    } else {
      if (log.isDebugEnabled()) {
        log.debug("BLTNODE.seek(), The iterator was null!");
      }
      this.setTopKey(null);
    }
  }
  
  public String buildTreePathString(TreeNode[] path) {
    StringBuilder s = new StringBuilder("[");
    for (TreeNode p : path) {
      s.append(p.toString());
      s.append(",");
    }
    s.deleteCharAt(s.length() - 1);
    s.append("]");
    return s.toString();
  }
  
  public void next() throws IOException {
    
    // always start fresh
    this.setTopKey(null);
    
    if (log.isDebugEnabled()) {
      TreeNode[] path = this.getPath();
      log.debug("BLTNODE.next() path-> " + this.buildTreePathString(path));
    }
    
    // have I been marked as done?
    if (this.isDone()) {
      if (log.isDebugEnabled()) {
        log.debug("I've been marked as done, returning");
      }
      return;
    }
    
    SortedKeyValueIterator<?,?> iter = (SortedKeyValueIterator<?,?>) this.getUserObject();
    iter.next();
    
    if (iter.hasTop()) {
      Key key = (Key) iter.getTopKey();
      
      // I have a valid topKey, pull out the piece I want
      key = buildKey(key);
      this.setTopKey(key);
      
      if (log.isDebugEnabled()) {
        log.debug("BLTNODE.next() -> found: " + this.getTopKey());
      }
    } else {
      // no top value has been returned, I'm done.
      if (log.isDebugEnabled()) {
        log.debug("BLTNODE.next() -> Nothing found");
      }
      this.setTopKey(null);
      this.setDone(true);
    }
    
  }
  
  public boolean jump(Key jumpKey) throws IOException {
    boolean ok = true;
    if (this.getType() == ParserTreeConstants.JJTEQNODE) {
      FieldIndexIterator iter = (FieldIndexIterator) this.getUserObject();
      ok = iter.jump(jumpKey);
      if (iter.hasTop()) {
        Key key = (Key) iter.getTopKey();
        key = buildKey(key);
        
        this.setTopKey(key);
        if (log.isDebugEnabled()) {
          log.debug("BLTNODE.jump() -> found: " + this.getTopKey());
        }
      } else {
        if (log.isDebugEnabled()) {
          log.debug("FieldIndexIteratorJexl does not have top after jump, marking done.");
        }
        this.setTopKey(null);
        this.setDone(true);
      }
      
    } else if (this.getType() == ParserTreeConstants.JJTANDNODE) {
      AndIterator iter = (AndIterator) this.getUserObject();
      ok = iter.jump(jumpKey);
      if (iter.hasTop()) {
        Key key = (Key) iter.getTopKey();
        key = buildKey(key);
        
        this.setTopKey(key);
        if (log.isDebugEnabled()) {
          log.debug("BLTNODE.jump() -> found: " + this.getTopKey());
        }
      } else {
        if (log.isDebugEnabled()) {
          log.debug("IntersectingIteratorJexl does not have top after jump, marking done.");
        }
        this.setTopKey(null);
        this.setDone(true);
      }
    } else if (this.getType() == ParserTreeConstants.JJTORNODE) {
      OrIterator iter = (OrIterator) this.getUserObject();
      ok = iter.jump(jumpKey);
      if (iter.hasTop()) {
        Key key = (Key) iter.getTopKey();
        key = buildKey(key);
        
        this.setTopKey(key);
        if (log.isDebugEnabled()) {
          log.debug("BLTNODE.jump() -> found: " + this.getTopKey());
        }
      } else {
        if (log.isDebugEnabled()) {
          log.debug("OrIteratorJexl does not have top after jump, marking done.");
        }
        this.setTopKey(null);
        this.setDone(true);
      }
    }
    return ok;
  }
  
  public void addToSet(Key i) {
    uids.add(i);
  }
  
  public void reSet() {
    uids = new HashSet<Key>();
  }
  
  public boolean inSet(Key t) {
    return uids.contains(t);
  }
  
  public Iterator<Key> getSetIterator() {
    return uids.iterator();
  }
  
  public HashSet<Key> getIntersection(HashSet<Key> h) {
    h.retainAll(uids);
    return h;
  }
  
  public Key getMinUniqueID() {
    Iterator<Key> iter = uids.iterator();
    Key min = null;
    while (iter.hasNext()) {
      Key t = (Key) iter.next();
      if (log.isDebugEnabled()) {
        log.debug("OR set member: " + t);
      }
      if (t != null) {
        if (min == null) {
          min = t;
        } else if (t.compareTo(min) < 0) {
          min = t;
        }
      }
    }
    return min;
  }
  
  public boolean hasTop() {
    // This part really needs to be cleaned up.
    // It was created before I knew what was being passed back.
    if (this.getType() == ParserTreeConstants.JJTORNODE) {
      // Are you a Logical OR or an OR Iterator
      if (!this.isLeaf()) { // logical construct
        // I have a set of keys
        return this.uids.size() > 0;
      } else { // or iterator, you only have possible key
        if (this.getTopKey() == null) {
          return false;
        } else {
          return true;
        }
      }
    } else {
      return this.getTopKey() != null;
    }
  }
  
  public static Key buildKey(Key key) {
    if (key == null) {
      log.error("Problem in BooleanLogicTreeNodeJexl.buildKey");
      return null;
    }
    // Build Key(Text row, Text colfam) where colFam is dataype\0uid
    String[] cq = key.getColumnQualifier().toString().split("\0");
    Text uuid = new Text(cq[cq.length - 2] + "\0" + cq[cq.length - 1]);
    Text row = key.getRow();
    if (log.isDebugEnabled()) {
      log.debug("Key-> r:" + row + "  fam:" + uuid);
    }
    // System.out.println("Key-> r:"+row+"  fam:"+uuid);
    
    Key k = new Key(row, uuid);
    // System.out.println("Key: "+k);
    return k;
  }
}
