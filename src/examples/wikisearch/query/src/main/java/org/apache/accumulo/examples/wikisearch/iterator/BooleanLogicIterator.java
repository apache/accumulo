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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.examples.wikisearch.parser.JexlOperatorConstants;
import org.apache.accumulo.examples.wikisearch.parser.QueryParser;
import org.apache.accumulo.examples.wikisearch.parser.QueryParser.QueryTerm;
import org.apache.accumulo.examples.wikisearch.parser.RangeCalculator.RangeBounds;
import org.apache.accumulo.examples.wikisearch.parser.TreeNode;
import org.apache.accumulo.examples.wikisearch.util.FieldIndexKeyParser;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;

public class BooleanLogicIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
  
  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();
  protected static final Logger log = Logger.getLogger(BooleanLogicIterator.class);
  public static final String QUERY_OPTION = "expr";
  public static final String TERM_CARDINALITIES = "TERM_CARDINALITIES"; // comma separated list of term : count
  public static final String FIELD_INDEX_QUERY = "FIELD_INDEX_QUERY";
  public static final String FIELD_NAME_PREFIX = "fi\0";
  // --------------------------------------------------------------------------
  private static IteratorEnvironment env = new DefaultIteratorEnvironment();
  protected Text nullText = new Text();
  private Key topKey = null;
  private Value topValue = null;
  private SortedKeyValueIterator<Key,Value> sourceIterator;
  private BooleanLogicTreeNode root;
  private PriorityQueue<BooleanLogicTreeNode> positives;
  private ArrayList<BooleanLogicTreeNode> negatives = new ArrayList<BooleanLogicTreeNode>();
  private ArrayList<BooleanLogicTreeNode> rangerators;
  private String updatedQuery;
  private Map<String,Long> termCardinalities = new HashMap<String,Long>();
  private Range overallRange = null;
  private FieldIndexKeyParser keyParser;
  
  public BooleanLogicIterator() {
    keyParser = new FieldIndexKeyParser();
    rangerators = new ArrayList<BooleanLogicTreeNode>();
  }
  
  public BooleanLogicIterator(BooleanLogicIterator other, IteratorEnvironment env) {
    if (other.sourceIterator != null) {
      this.sourceIterator = other.sourceIterator.deepCopy(env);
    }
    keyParser = new FieldIndexKeyParser();
    rangerators = new ArrayList<BooleanLogicTreeNode>();
    log.debug("Congratulations, you've reached the BooleanLogicIterator");
  }
  
  public static void setLogLevel(Level lev) {
    log.setLevel(lev);
  }
  
  public void setDebug(Level lev) {
    log.setLevel(lev);
  }
  
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new BooleanLogicIterator(this, env);
  }
  
  /**
   * <b>init</b> is responsible for setting up the iterator. It will pull the serialized boolean parse tree from the options mapping and construct the
   * appropriate sub-iterators
   * 
   * Once initialized, this iterator will automatically seek to the first matching instance. If no top key exists, that means an event matching the boolean
   * logic did not exist in the partition. Subsequent calls to next will move the iterator and all sub-iterators to the next match.
   * 
   * @param source
   *          The underlying SortedkeyValueIterator.
   * @param options
   *          A Map<String, String> of options.
   * @param env
   *          The iterator environment
   * @throws IOException
   */
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    validateOptions(options);
    try {
      if (log.isDebugEnabled()) {
        log.debug("Congratulations, you've reached the BooleanLogicIterator.init method");
      }
      // Copy the source iterator
      sourceIterator = source.deepCopy(env);
      
      // Potentially take advantage of term cardinalities
      String[] terms = null;
      if (null != options.get(TERM_CARDINALITIES)) {
        terms = options.get(TERM_CARDINALITIES).split(",");
        for (String term : terms) {
          int idx = term.indexOf(":");
          if (-1 != idx) {
            termCardinalities.put(term.substring(0, idx), Long.parseLong(term.substring(idx + 1)));
          }
        }
      }
      
      // Step 1: Parse the query
      if (log.isDebugEnabled()) {
        log.debug("QueryParser");
      }
      QueryParser qp = new QueryParser();
      qp.execute(this.updatedQuery); // validateOptions updates the updatedQuery
      
      // need to build the query tree based on jexl parsing.
      // Step 2: refactor QueryTree - inplace modification
      if (log.isDebugEnabled()) {
        log.debug("transformTreeNode");
      }
      TreeNode tree = qp.getIteratorTree();
      this.root = transformTreeNode(tree);
      
      if (log.isDebugEnabled()) {
        log.debug("refactorTree");
      }
      this.root = refactorTree(this.root);
      
      if (log.isDebugEnabled()) {
        log.debug("collapseBranches");
      }
      collapseBranches(root);
      
      // Step 3: create iterators where we need them.
      createIteratorTree(this.root);
      if (log.isDebugEnabled()) {
        log.debug("Query tree after iterator creation:\n\t" + this.root.getContents());
      }
      // Step 4: split the positive and negative leaves
      splitLeaves(this.root);
      
    } catch (ParseException ex) {
      log.error("ParseException in init: " + ex);
      throw new IllegalArgumentException("Failed to parse query", ex);
    } catch (Exception ex) {
      throw new IllegalArgumentException("probably had no indexed terms", ex);
    }
    
  }
  
  /* *************************************************************************
   * Methods for sub iterator creation.
   */
  private void createIteratorTree(BooleanLogicTreeNode root) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("BoolLogic createIteratorTree()");
    }
    // Walk the tree, if all of your children are leaves, roll you into the
    // appropriate iterator.
    Enumeration<?> dfe = root.depthFirstEnumeration();
    
    while (dfe.hasMoreElements()) {
      BooleanLogicTreeNode node = (BooleanLogicTreeNode) dfe.nextElement();
      if (!node.isLeaf() && node.getType() != ParserTreeConstants.JJTJEXLSCRIPT) {
        // try to roll up.
        if (canRollUp(node)) {
          node.setRollUp(true);
          if (node.getType() == ParserTreeConstants.JJTANDNODE) {
            if (log.isDebugEnabled()) {
              log.debug("creating IntersectingIterator");
            }
            node.setUserObject(createIntersectingIterator(node));
          } else if (node.getType() == ParserTreeConstants.JJTORNODE) {
            node.setUserObject(createOrIterator(node));
          } else {
            // throw an error.
            log.debug("createIteratorTree, encounterd a node type I do not know about: " + node.getType());
            log.debug("createIteratorTree, node contents:  " + node.getContents());
          }
          node.removeAllChildren();
        }
      }
    }
    
    // now for remaining leaves, create basic iterators.
    // you can add in specialized iterator mappings here if necessary.
    dfe = root.depthFirstEnumeration();
    while (dfe.hasMoreElements()) {
      BooleanLogicTreeNode node = (BooleanLogicTreeNode) dfe.nextElement();
      if (node.isLeaf() && node.getType() != ParserTreeConstants.JJTANDNODE && node.getType() != ParserTreeConstants.JJTORNODE) {
        node.setUserObject(createFieldIndexIterator(node));
      }
    }
  }
  
  private AndIterator createIntersectingIterator(BooleanLogicTreeNode node) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("createIntersectingIterator(node)");
      log.debug("fName: " + node.getFieldName() + " , fValue: " + node.getFieldValue() + " , operator: " + node.getFieldOperator());
    }
    Text[] columnFamilies = new Text[node.getChildCount()];
    Text[] termValues = new Text[node.getChildCount()];
    boolean[] negationMask = new boolean[node.getChildCount()];
    Enumeration<?> children = node.children();
    int i = 0;
    while (children.hasMoreElements()) {
      BooleanLogicTreeNode child = (BooleanLogicTreeNode) children.nextElement();
      columnFamilies[i] = child.getFieldName();
      termValues[i] = child.getFieldValue();
      negationMask[i] = child.isNegated();
      i++;
    }
    
    AndIterator ii = new AndIterator();
    Map<String,String> options = new HashMap<String,String>();
    options.put(AndIterator.columnFamiliesOptionName, AndIterator.encodeColumns(columnFamilies));
    options.put(AndIterator.termValuesOptionName, AndIterator.encodeTermValues(termValues));
    options.put(AndIterator.notFlagsOptionName, AndIterator.encodeBooleans(negationMask));
    
    ii.init(sourceIterator.deepCopy(env), options, env);
    return ii;
  }
  
  private OrIterator createOrIterator(BooleanLogicTreeNode node) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("createOrIterator(node)");
      log.debug("fName: " + node.getFieldName() + " , fValue: " + node.getFieldValue() + " , operator: " + node.getFieldOperator());
    }
    
    Enumeration<?> children = node.children();
    ArrayList<Text> fams = new ArrayList<Text>();
    ArrayList<Text> quals = new ArrayList<Text>();
    while (children.hasMoreElements()) {
      BooleanLogicTreeNode child = (BooleanLogicTreeNode) children.nextElement();
      fams.add(child.getFieldName());
      quals.add(child.getFieldValue());
    }
    
    OrIterator iter = new OrIterator();
    SortedKeyValueIterator<Key,Value> source = sourceIterator.deepCopy(env);
    for (int i = 0; i < fams.size(); i++) {
      iter.addTerm(source, fams.get(i), quals.get(i), env);
    }
    
    return iter;
  }
  
  /*
   * This takes the place of the SortedKeyIterator used previously. This iterator is bound to the partitioned table structure. When next is called it will jump
   * rows as necessary internally versus needing to do it externally as was the case with the SortedKeyIterator.
   */
  private FieldIndexIterator createFieldIndexIterator(BooleanLogicTreeNode node) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("BoolLogic.createFieldIndexIterator()");
      log.debug("fName: " + node.getFieldName() + " , fValue: " + node.getFieldValue() + " , operator: " + node.getFieldOperator());
    }
    Text rowId = null;
    sourceIterator.seek(new Range(), EMPTY_COL_FAMS, false);
    if (sourceIterator.hasTop()) {
      rowId = sourceIterator.getTopKey().getRow();
    }
    
    FieldIndexIterator iter = new FieldIndexIterator(node.getType(), rowId, node.getFieldName(), node.getFieldValue(), node.isNegated(),
        node.getFieldOperator());
    
    Map<String,String> options = new HashMap<String,String>();
    iter.init(sourceIterator.deepCopy(env), options, env);
    if (log.isDebugEnabled()) {
      FieldIndexIterator.setLogLevel(Level.DEBUG);
    } else {
      FieldIndexIterator.setLogLevel(Level.OFF);
    }
    
    return iter;
  }
  
  /* *************************************************************************
   * Methods for testing the tree WRT boolean logic.
   */
  // After all iterator pointers have been advanced, test if the current
  // record passes the boolean logic.
  private boolean testTreeState() {
    if (log.isDebugEnabled()) {
      log.debug("BoolLogic testTreeState() begin");
    }
    Enumeration<?> dfe = this.root.depthFirstEnumeration();
    while (dfe.hasMoreElements()) {
      BooleanLogicTreeNode node = (BooleanLogicTreeNode) dfe.nextElement();
      if (!node.isLeaf()) {
        
        int type = node.getType();
        if (type == ParserTreeConstants.JJTANDNODE) { // BooleanLogicTreeNode.NodeType.AND) {
          handleAND(node);
        } else if (type == ParserTreeConstants.JJTORNODE) {// BooleanLogicTreeNode.NodeType.OR) {
          handleOR(node);
        } else if (type == ParserTreeConstants.JJTJEXLSCRIPT) {// BooleanLogicTreeNode.NodeType.HEAD) {
          handleHEAD(node);
        } else if (type == ParserTreeConstants.JJTNOTNODE) { // BooleanLogicTreeNode.NodeType.NOT) {
          // there should not be any "NOT"s.
          // throw new Exception();
        }
      } else {
        // it is a leaf, if it is an AND or OR do something
        if (node.getType() == ParserTreeConstants.JJTORNODE) {// BooleanLogicTreeNode.NodeType.OR) { //OrIterator
          node.setValid(node.hasTop());
          node.reSet();
          node.addToSet(node.getTopKey());
          
        } else if (node.getType() == ParserTreeConstants.JJTANDNODE || node.getType() == ParserTreeConstants.JJTEQNODE
            || node.getType() == ParserTreeConstants.JJTERNODE || node.getType() == ParserTreeConstants.JJTLENODE
            || node.getType() == ParserTreeConstants.JJTLTNODE || node.getType() == ParserTreeConstants.JJTGENODE
            || node.getType() == ParserTreeConstants.JJTGTNODE) {
          // sub iterator guarantees it is in its internal range,
          // otherwise, no top.
          node.setValid(node.hasTop());
        }
      }
    }
    
    if (log.isDebugEnabled()) {
      log.debug("BoolLogic.testTreeState end, treeState:: " + this.root.getContents() + "  , valid: " + root.isValid());
    }
    return this.root.isValid();
  }
  
  private void handleHEAD(BooleanLogicTreeNode node) {
    Enumeration<?> children = node.children();
    while (children.hasMoreElements()) {
      BooleanLogicTreeNode child = (BooleanLogicTreeNode) children.nextElement();
      
      if (child.getType() == ParserTreeConstants.JJTANDNODE) {// BooleanLogicTreeNode.NodeType.AND) {
        node.setValid(child.isValid());
        node.setTopKey(child.getTopKey());
      } else if (child.getType() == ParserTreeConstants.JJTORNODE) {// BooleanLogicTreeNode.NodeType.OR) {
        node.setValid(child.isValid());
        node.setTopKey(child.getTopKey());
      } else if (child.getType() == ParserTreeConstants.JJTEQNODE || child.getType() == ParserTreeConstants.JJTERNODE
          || child.getType() == ParserTreeConstants.JJTGTNODE || child.getType() == ParserTreeConstants.JJTGENODE
          || child.getType() == ParserTreeConstants.JJTLTNODE || child.getType() == ParserTreeConstants.JJTLENODE) {// BooleanLogicTreeNode.NodeType.SEL) {
        node.setValid(true);
        node.setTopKey(child.getTopKey());
        if (child.getTopKey() == null) {
          node.setValid(false);
        }
      }
    }// end while
    
    // I have to be valid AND have a top key
    if (node.isValid() && !node.hasTop()) {
      node.setValid(false);
    }
  }
  
  private void handleAND(BooleanLogicTreeNode me) {
    if (log.isDebugEnabled()) {
      log.debug("handleAND::" + me.getContents());
    }
    Enumeration<?> children = me.children();
    me.setValid(true); // it's easier to prove false than true
    
    HashSet<Key> goodSet = new HashSet<Key>();
    HashSet<Key> badSet = new HashSet<Key>();
    while (children.hasMoreElements()) {
      BooleanLogicTreeNode child = (BooleanLogicTreeNode) children.nextElement();
      
      if (child.getType() == ParserTreeConstants.JJTEQNODE || child.getType() == ParserTreeConstants.JJTANDNODE
          || child.getType() == ParserTreeConstants.JJTERNODE || child.getType() == ParserTreeConstants.JJTNENODE
          || child.getType() == ParserTreeConstants.JJTGENODE || child.getType() == ParserTreeConstants.JJTLENODE
          || child.getType() == ParserTreeConstants.JJTGTNODE || child.getType() == ParserTreeConstants.JJTLTNODE) {
        
        if (child.isNegated()) {
          if (child.hasTop()) {
            badSet.add(child.getTopKey());
            if (goodSet.contains(child.getTopKey())) {
              me.setValid(false);
              return;
            }
            if (child.isValid()) {
              me.setValid(false);
              return;
            }
          }
        } else {
          if (child.hasTop()) {
            if (log.isDebugEnabled()) {
              log.debug("handleAND, child node: " + child.getContents());
            }
            // if you're in the bad set, you're done.
            if (badSet.contains(child.getTopKey())) {
              if (log.isDebugEnabled()) {
                log.debug("handleAND, child is in bad set, setting parent false");
              }
              me.setValid(false);
              return;
            }
            
            // if good set is empty, add it.
            if (goodSet.isEmpty()) {
              if (log.isDebugEnabled()) {
                log.debug("handleAND, goodSet is empty, adding child: " + child.getContents());
              }
              goodSet.add(child.getTopKey());
            } else {
              // must be in the good set & not in the bad set
              // if either fails, I'm false.
              if (!goodSet.contains(child.getTopKey())) {
                if (log.isDebugEnabled()) {
                  log.debug("handleAND, goodSet is not empty, and does NOT contain child, setting false.  child: " + child.getContents());
                }
                me.setValid(false);
                return;
              } else {
                // trim the good set to this one value
                // (handles the case were the initial encounters were ORs)
                goodSet = new HashSet<Key>();
                goodSet.add(child.getTopKey());
                if (log.isDebugEnabled()) {
                  log.debug("handleAND, child in goodset, trim to this value: " + child.getContents());
                }
              }
            }
          } else {
            // test if its children are all false
            if (child.getChildCount() > 0) {
              Enumeration<?> subchildren = child.children();
              boolean allFalse = true;
              while (subchildren.hasMoreElements()) {
                BooleanLogicTreeNode subchild = (BooleanLogicTreeNode) subchildren.nextElement();
                if (!subchild.isNegated()) {
                  allFalse = false;
                  break;
                } else if (subchild.isNegated() && subchild.hasTop()) {
                  allFalse = false;
                  break;
                }
              }
              if (!allFalse) {
                me.setValid(false);
                return;
              }
            } else {
              // child returned a null value and is not a negation, this in turn makes me false.
              me.setValid(false);
              return;
            }
          }
        }
        
      } else if (child.getType() == ParserTreeConstants.JJTORNODE) {// BooleanLogicTreeNode.NodeType.OR) {
      
        // NOTE: The OR may be an OrIterator in which case it will only produce
        // a single unique identifier, or it may be a pure logical construct and
        // be capable of producing multiple unique identifiers.
        // This should handle all cases.
        Iterator<?> iter = child.getSetIterator();
        boolean goodSetEmpty = goodSet.isEmpty();
        boolean matchedOne = false;
        boolean pureNegations = true;
        if (!child.isValid()) {
          if (log.isDebugEnabled()) {
            log.debug("handleAND, child is an OR and it is not valid, setting false, ALL NEGATED?: " + child.isChildrenAllNegated());
          }
          me.setValid(false); // I'm an AND if one of my children is false, I'm false.
          return;
        } else if (child.isValid() && !child.hasTop()) {
          // pure negation, do nothing
        } else if (child.isValid() && child.hasTop()) { // I need to match one
          if (log.isDebugEnabled()) {
            log.debug("handleAND, child OR, valid and has top, means not pureNegations");
          }
          pureNegations = false;
          while (iter.hasNext()) {
            Key i = (Key) iter.next();
            if (child.isNegated()) {
              badSet.add(i);
              if (goodSet.contains(i)) {
                if (log.isDebugEnabled()) {
                  log.debug("handleAND, child OR, goodSet contains bad value: " + i);
                }
                me.setValid(false);
                return;
              }
            } else {
              // if the good set is empty, then push all of my ids.
              if (goodSetEmpty && !badSet.contains(i)) {
                goodSet.add(i);
                matchedOne = true;
              } else {
                // I need at least one to match
                if (goodSet.contains(i)) {
                  matchedOne = true;
                }
              }
            }
          }
        }
        
        // is the goodSet still empty? that means were were only negations
        // otherwise, if it's not empty and we didn't match one, false
        if (child.isNegated()) {
          // we're ok
        } else {
          if (goodSet.isEmpty() && !pureNegations) {
            if (log.isDebugEnabled()) {
              log.debug("handleAND, child OR, empty goodset && !pureNegations, set false");
            }
            // that's bad, we weren't negated, should've pushed something in there.
            me.setValid(false);
            return;
          } else if (!goodSet.isEmpty() && !pureNegations) { // goodSet contains values.
            if (!matchedOne) { // but we didn't match any.
              if (log.isDebugEnabled()) {
                log.debug("handleAND, child OR, goodSet had values but I didn't match any, false");
              }
              me.setValid(false);
              return;
            }
            
            // we matched something, trim the set.
            // i.e. two child ORs
            goodSet = child.getIntersection(goodSet);
          }
        }
        
      }
    }// end while
    
    if (goodSet.isEmpty()) { // && log.isDebugEnabled()) {
      if (log.isDebugEnabled()) {
        log.debug("handleAND-> goodSet is empty, pure negations?");
      }
    } else {
      me.setTopKey(Collections.min(goodSet));
      if (log.isDebugEnabled()) {
        log.debug("End of handleAND, this node's topKey: " + me.getTopKey());
      }
    }
  }
  
  private void handleOR(BooleanLogicTreeNode me) {
    Enumeration<?> children = me.children();
    // I'm an OR node, need at least one positive.
    me.setValid(false);
    me.reSet();
    me.setTopKey(null);
    boolean allNegated = true;
    while (children.hasMoreElements()) {
      // 3 cases for child: SEL, AND, OR
      // and negation
      BooleanLogicTreeNode child = (BooleanLogicTreeNode) children.nextElement();
      if (child.getType() == ParserTreeConstants.JJTEQNODE || child.getType() == ParserTreeConstants.JJTNENODE
          || child.getType() == ParserTreeConstants.JJTANDNODE || child.getType() == ParserTreeConstants.JJTERNODE
          || child.getType() == ParserTreeConstants.JJTNRNODE || child.getType() == ParserTreeConstants.JJTLENODE
          || child.getType() == ParserTreeConstants.JJTLTNODE || child.getType() == ParserTreeConstants.JJTGENODE
          || child.getType() == ParserTreeConstants.JJTGTNODE) {
        
        if (child.hasTop()) {
          if (child.isNegated()) {
            // do nothing.
          } else {
            allNegated = false;
            // I have something add it to my set.
            if (child.isValid()) {
              me.addToSet(child.getTopKey());
            }
          }
        } else if (!child.isNegated()) { // I have a non-negated child
          allNegated = false;
          // that child could be pure negations in which case I'm true
          me.setValid(child.isValid());
        }
        
      } else if (child.getType() == ParserTreeConstants.JJTORNODE) {// BooleanLogicTreeNode.NodeType.OR) {
        if (child.hasTop()) {
          if (!child.isNegated()) {
            allNegated = false;
            // add its rowIds to my rowIds
            Iterator<?> iter = child.getSetIterator();
            while (iter.hasNext()) {
              Key i = (Key) iter.next();
              if (i != null) {
                me.addToSet(i);
              }
            }
          }
        } else {
          // Or node that doesn't have a top, check if it's valid or not
          // because it could be pure negations itself.
          if (child.isValid()) {
            me.setValid(true);
          }
        }
      }
    }// end while
    
    if (allNegated) {
      // do all my children have top?
      children = me.children();
      while (children.hasMoreElements()) {
        BooleanLogicTreeNode child = (BooleanLogicTreeNode) children.nextElement();
        if (!child.hasTop()) {
          me.setValid(true);
          me.setTopKey(null);
          return;
        }
      }
      me.setValid(false);
      
    } else {
      Key k = me.getMinUniqueID();
      if (k == null) {
        me.setValid(false);
      } else {
        me.setValid(true);
        me.setTopKey(k);
      }
    }
  }
  
  /* *************************************************************************
   * Utility methods.
   */
  // Transforms the TreeNode tree of query.parser into the
  // BooleanLogicTreeNodeJexl form.
  public BooleanLogicTreeNode transformTreeNode(TreeNode node) throws ParseException {
    if (node.getType().equals(ASTEQNode.class) || node.getType().equals(ASTNENode.class)) {
      if (log.isDebugEnabled()) {
        log.debug("Equals Node");
      }
      
      Multimap<String,QueryTerm> terms = node.getTerms();
      for (String fName : terms.keySet()) {
        Collection<QueryTerm> values = terms.get(fName);
        
        for (QueryTerm t : values) {
          if (null == t || null == t.getValue()) {
            continue;
          }
          String fValue = t.getValue().toString();
          fValue = fValue.replaceAll("'", "");
          boolean negated = t.getOperator().equals("!=");
          
          if (!fName.startsWith(FIELD_NAME_PREFIX)) {
            fName = FIELD_NAME_PREFIX + fName;
          }
          BooleanLogicTreeNode child = new BooleanLogicTreeNode(ParserTreeConstants.JJTEQNODE, fName, fValue, negated);
          return child;
        }
      }
    }
    
    if (node.getType().equals(ASTERNode.class) || node.getType().equals(ASTNRNode.class)) {
      if (log.isDebugEnabled()) {
        log.debug("Regex Node");
      }
      
      Multimap<String,QueryTerm> terms = node.getTerms();
      for (String fName : terms.keySet()) {
        Collection<QueryTerm> values = terms.get(fName);
        for (QueryTerm t : values) {
          if (null == t || null == t.getValue()) {
            continue;
          }
          String fValue = t.getValue().toString();
          fValue = fValue.replaceAll("'", "");
          boolean negated = node.getType().equals(ASTNRNode.class);
          
          if (!fName.startsWith(FIELD_NAME_PREFIX)) {
            fName = FIELD_NAME_PREFIX + fName;
          }
          
          BooleanLogicTreeNode child = new BooleanLogicTreeNode(ParserTreeConstants.JJTERNODE, fName, fValue, negated);
          return child;
        }
      }
    }
    
    if (node.getType().equals(ASTLTNode.class) || node.getType().equals(ASTLENode.class) || node.getType().equals(ASTGTNode.class)
        || node.getType().equals(ASTGENode.class)) {
      Multimap<String,QueryTerm> terms = node.getTerms();
      for (String fName : terms.keySet()) {
        Collection<QueryTerm> values = terms.get(fName);
        
        if (!fName.startsWith(FIELD_NAME_PREFIX)) {
          fName = FIELD_NAME_PREFIX + fName;
        }
        for (QueryTerm t : values) {
          if (null == t || null == t.getValue()) {
            continue;
          }
          String fValue = t.getValue().toString();
          fValue = fValue.replaceAll("'", "").toLowerCase();
          boolean negated = false; // to be negated, must be child of Not, which is handled elsewhere.
          int mytype = JexlOperatorConstants.getJJTNodeType(t.getOperator());
          
          BooleanLogicTreeNode child = new BooleanLogicTreeNode(mytype, fName, fValue, negated);
          if (log.isDebugEnabled()) {
            log.debug("adding child node: " + child.getContents());
          }
          return child;
        }
      }
    }
    
    BooleanLogicTreeNode returnNode = null;
    
    if (node.getType().equals(ASTAndNode.class) || node.getType().equals(ASTOrNode.class)) {
      int parentType = node.getType().equals(ASTAndNode.class) ? ParserTreeConstants.JJTANDNODE : ParserTreeConstants.JJTORNODE;
      if (log.isDebugEnabled()) {
        log.debug("AND/OR node: " + parentType);
      }
      if (node.isLeaf() || !node.getTerms().isEmpty()) {
        returnNode = new BooleanLogicTreeNode(parentType);
        Multimap<String,QueryTerm> terms = node.getTerms();
        for (String fName : terms.keySet()) {
          Collection<QueryTerm> values = terms.get(fName);
          if (!fName.startsWith(FIELD_NAME_PREFIX)) {
            fName = FIELD_NAME_PREFIX + fName;
          }
          for (QueryTerm t : values) {
            if (null == t || null == t.getValue()) {
              continue;
            }
            String fValue = t.getValue().toString();
            fValue = fValue.replaceAll("'", "");
            boolean negated = t.getOperator().equals("!=");
            int mytype = JexlOperatorConstants.getJJTNodeType(t.getOperator());
            BooleanLogicTreeNode child = new BooleanLogicTreeNode(mytype, fName, fValue, negated);
            if (log.isDebugEnabled()) {
              log.debug("adding child node: " + child.getContents());
            }
            returnNode.add(child);
          }
        }
      } else {
        returnNode = new BooleanLogicTreeNode(parentType);
        
      }
    } else if (node.getType().equals(ASTNotNode.class)) {
      if (log.isDebugEnabled()) {
        log.debug("NOT node");
      }
      if (node.isLeaf()) {
        // NOTE: this should be cleaned up a bit.
        Multimap<String,QueryTerm> terms = node.getTerms();
        for (String fName : terms.keySet()) {
          Collection<QueryTerm> values = terms.get(fName);
          
          if (!fName.startsWith(FIELD_NAME_PREFIX)) {
            fName = FIELD_NAME_PREFIX + fName;
          }
          for (QueryTerm t : values) {
            if (null == t || null == t.getValue()) {
              continue;
            }
            String fValue = t.getValue().toString();
            fValue = fValue.replaceAll("'", "").toLowerCase();
            boolean negated = !t.getOperator().equals("!=");
            int mytype = JexlOperatorConstants.getJJTNodeType(t.getOperator());
            
            if (!fName.startsWith(FIELD_NAME_PREFIX)) {
              fName = FIELD_NAME_PREFIX + fName;
            }
            return new BooleanLogicTreeNode(mytype, fName, fValue, negated);
          }
        }
      } else {
        returnNode = new BooleanLogicTreeNode(ParserTreeConstants.JJTNOTNODE);
      }
    } else if (node.getType().equals(ASTJexlScript.class) || node.getType().getSimpleName().equals("RootNode")) {
      
      if (log.isDebugEnabled()) {
        log.debug("ROOT/JexlScript node");
      }
      if (node.isLeaf()) {
        returnNode = new BooleanLogicTreeNode(ParserTreeConstants.JJTJEXLSCRIPT);
        // NOTE: this should be cleaned up a bit.
        Multimap<String,QueryTerm> terms = node.getTerms();
        for (String fName : terms.keySet()) {
          Collection<QueryTerm> values = terms.get(fName);
          
          if (!fName.startsWith(FIELD_NAME_PREFIX)) {
            fName = FIELD_NAME_PREFIX + fName;
          }
          for (QueryTerm t : values) {
            if (null == t || null == t.getValue()) {
              continue;
            }
            String fValue = t.getValue().toString();
            fValue = fValue.replaceAll("'", "").toLowerCase();
            boolean negated = t.getOperator().equals("!=");
            int mytype = JexlOperatorConstants.getJJTNodeType(t.getOperator());
            
            BooleanLogicTreeNode child = new BooleanLogicTreeNode(mytype, fName, fValue, negated);
            returnNode.add(child);
            return returnNode;
          }
        }
      } else {
        returnNode = new BooleanLogicTreeNode(ParserTreeConstants.JJTJEXLSCRIPT);
      }
    } else {
      log.error("Currently Unsupported Node type: " + node.getClass().getName() + " \t" + node.getType());
    }
    for (TreeNode child : node.getChildren()) {
      returnNode.add(transformTreeNode(child));
    }
    
    return returnNode;
  }
  
  // After tree conflicts have been resolve, we can collapse branches where
  // leaves have been pruned.
  public static void collapseBranches(BooleanLogicTreeNode myroot) throws Exception {
    
    // NOTE: doing a depth first enumeration didn't wory when I started
    // removing nodes halfway through. The following method does work,
    // it's essentially a reverse breadth first traversal.
    List<BooleanLogicTreeNode> nodes = new ArrayList<BooleanLogicTreeNode>();
    Enumeration<?> bfe = myroot.breadthFirstEnumeration();
    
    while (bfe.hasMoreElements()) {
      BooleanLogicTreeNode node = (BooleanLogicTreeNode) bfe.nextElement();
      nodes.add(node);
    }
    
    // walk backwards
    for (int i = nodes.size() - 1; i >= 0; i--) {
      BooleanLogicTreeNode node = nodes.get(i);
      if (log.isDebugEnabled()) {
        log.debug("collapseBranches, inspecting node: " + node.toString() + "  " + node.printNode());
      }
      
      if (node.getType() == ParserTreeConstants.JJTANDNODE || node.getType() == ParserTreeConstants.JJTORNODE) {
        if (node.getChildCount() == 0 && !node.isRangeNode()) {
          node.removeFromParent();
        } else if (node.getChildCount() == 1) {
          BooleanLogicTreeNode p = (BooleanLogicTreeNode) node.getParent();
          BooleanLogicTreeNode c = (BooleanLogicTreeNode) node.getFirstChild();
          node.removeFromParent();
          p.add(c);
          
        }
      } else if (node.getType() == ParserTreeConstants.JJTJEXLSCRIPT) {
        if (node.getChildCount() == 0) {
          if (log.isDebugEnabled()) {
            log.debug("collapseBranches, headNode has no children");
          }
          throw new Exception("Head node has no children.");
        }
      }
    }
    
  }
  
  public BooleanLogicTreeNode refactorTree(BooleanLogicTreeNode myroot) {
    List<BooleanLogicTreeNode> nodes = new ArrayList<BooleanLogicTreeNode>();
    Enumeration<?> bfe = myroot.breadthFirstEnumeration();
    
    while (bfe.hasMoreElements()) {
      BooleanLogicTreeNode node = (BooleanLogicTreeNode) bfe.nextElement();
      nodes.add(node);
    }
    
    // walk backwards
    for (int i = nodes.size() - 1; i >= 0; i--) {
      BooleanLogicTreeNode node = nodes.get(i);
      if (node.getType() == ParserTreeConstants.JJTANDNODE || node.getType() == ParserTreeConstants.JJTORNODE) {
        // 1. check to see if all children are negated
        // 2. check to see if we have to handle ranges.
        
        Map<Text,RangeBounds> ranges = new HashMap<Text,RangeBounds>();
        Enumeration<?> children = node.children();
        boolean allNegated = true;
        while (children.hasMoreElements()) {
          BooleanLogicTreeNode child = (BooleanLogicTreeNode) children.nextElement();
          if (!child.isNegated()) {
            allNegated = false;
            // break;
          }
          
          // currently we are not allowing unbounded ranges, so they must sit under an AND node.
          if (node.getType() == ParserTreeConstants.JJTANDNODE) {
            // check for ranges
            if (child.getType() == JexlOperatorConstants.JJTGTNODE) {
              if (log.isDebugEnabled()) {
                log.debug("refactor: GT " + child.getContents());
              }
              if (ranges.containsKey(child.getFieldName())) {
                RangeBounds rb = ranges.get(child.getFieldName());
                rb.setLower(child.getFieldValue());
              } else {
                RangeBounds rb = new RangeBounds();
                rb.setLower(child.getFieldValue());
                ranges.put(child.getFieldName(), rb);
              }
            } else if (child.getType() == JexlOperatorConstants.JJTGENODE) {
              if (log.isDebugEnabled()) {
                log.debug("refactor: GE " + child.getContents());
              }
              if (ranges.containsKey(child.getFieldName())) {
                RangeBounds rb = ranges.get(child.getFieldName());
                rb.setLower(child.getFieldValue());
              } else {
                RangeBounds rb = new RangeBounds();
                rb.setLower(child.getFieldValue());
                ranges.put(child.getFieldName(), rb);
              }
            } else if (child.getType() == JexlOperatorConstants.JJTLTNODE) {
              if (log.isDebugEnabled()) {
                log.debug("refactor: LT " + child.getContents());
              }
              if (ranges.containsKey(child.getFieldName())) {
                RangeBounds rb = ranges.get(child.getFieldName());
                rb.setUpper(child.getFieldValue());
              } else {
                RangeBounds rb = new RangeBounds();
                rb.setUpper(child.getFieldValue());
                ranges.put(child.getFieldName(), rb);
              }
            } else if (child.getType() == JexlOperatorConstants.JJTLENODE) {
              if (log.isDebugEnabled()) {
                log.debug("refactor: LE " + child.getContents());
              }
              if (ranges.containsKey(child.getFieldName())) {
                RangeBounds rb = ranges.get(child.getFieldName());
                rb.setUpper(child.getFieldValue());
              } else {
                RangeBounds rb = new RangeBounds();
                rb.setUpper(child.getFieldValue());
                ranges.put(child.getFieldName(), rb);
              }
            }
          }
        }
        if (allNegated) {
          node.setChildrenAllNegated(true);
        }
        
        // see if the AND node had a range.
        if (node.getType() == ParserTreeConstants.JJTANDNODE) {
          
          // if(ranges.containsKey(node.getFieldName())){
          if (!ranges.isEmpty()) {
            // we have a range, process it
            if (node.getChildCount() <= 2 && ranges.size() == 1) {
              if (log.isDebugEnabled()) {
                log.debug("AND range 2 children or less");
              }
              // only has a range, modify the node
              node.setType(ParserTreeConstants.JJTORNODE);
              node.removeAllChildren();
              // RangeBounds rb = ranges.get(node.getFieldName());
              
              for (Entry<Text,RangeBounds> entry : ranges.entrySet()) {
                Text fName = entry.getKey();
                RangeBounds rb = entry.getValue();
                node.setFieldName(fName);
                node.setFieldValue(new Text(""));
                node.setLowerBound(rb.getLower());
                node.setUpperBound(rb.getUpper());
                node.setRangeNode(true);
              }
              
              rangerators.add(node);
              
              if (log.isDebugEnabled()) {
                log.debug("refactor: " + node.getContents());
                log.debug("refactor: " + node.getLowerBound() + "  " + node.getUpperBound());
              }
              
            } else {
              if (log.isDebugEnabled()) {
                log.debug("AND range more than 2 children");
              }
              // node has range plus other children, create another node from the range
              // remove lt,le,gt,ge from parent and push in a single node
              
              // removing nodes via enumeration doesn't work, push into a list
              // and walk backwards
              List<BooleanLogicTreeNode> temp = new ArrayList<BooleanLogicTreeNode>();
              Enumeration<?> e = node.children();
              while (e.hasMoreElements()) {
                BooleanLogicTreeNode c = (BooleanLogicTreeNode) e.nextElement();
                temp.add(c);
              }
              
              for (int j = temp.size() - 1; j >= 0; j--) {
                BooleanLogicTreeNode c = temp.get(j);
                if (c.getType() == JexlOperatorConstants.JJTLENODE || c.getType() == JexlOperatorConstants.JJTLTNODE
                    || c.getType() == JexlOperatorConstants.JJTGENODE || c.getType() == JexlOperatorConstants.JJTGTNODE) {
                  c.removeFromParent();
                }
              }
              
              for (Entry<Text,RangeBounds> entry : ranges.entrySet()) {
                Text fName = entry.getKey();
                BooleanLogicTreeNode nchild = new BooleanLogicTreeNode(ParserTreeConstants.JJTORNODE, fName.toString(), "");
                RangeBounds rb = entry.getValue();
                nchild.setFieldValue(new Text(""));
                nchild.setLowerBound(rb.getLower());
                nchild.setUpperBound(rb.getUpper());
                nchild.setRangeNode(true);
                node.add(nchild);
                rangerators.add(nchild);
              }
              
              if (log.isDebugEnabled()) {
                log.debug("refactor: " + node.getContents());
              }
            }
          }
        }
        
      }
    }
    
    return myroot;
    
  }
  
  // If all children are of type SEL, roll this up into an AND or OR node.
  private static boolean canRollUp(BooleanLogicTreeNode parent) {
    if (log.isDebugEnabled()) {
      log.debug("canRollUp: testing " + parent.getContents());
    }
    if (parent.getChildCount() < 1) {
      if (log.isDebugEnabled()) {
        log.debug("canRollUp: child count < 1, return false");
      }
      return false;
    }
    Enumeration<?> e = parent.children();
    while (e.hasMoreElements()) {
      BooleanLogicTreeNode child = (BooleanLogicTreeNode) e.nextElement();
      
      if (child.getType() != ParserTreeConstants.JJTEQNODE) {// BooleanLogicTreeNode.NodeType.SEL) {
        if (log.isDebugEnabled()) {
          log.debug("canRollUp: child.getType -> " + ParserTreeConstants.jjtNodeName[child.getType()] + " int: " + child.getType() + "  return false");
        }
        return false;
      }
      
      if (child.isNegated()) {
        if (log.isDebugEnabled()) {
          log.debug("canRollUp: child.isNegated, return false");
        }
        return false;
      }
      
      if (child.getFieldValue().toString().contains("*")) {
        if (log.isDebugEnabled()) {
          log.debug("canRollUp: child has wildcard: " + child.getFieldValue());
        }
        return false;
      }
    }
    return true;
  }
  
  /**
   * Small utility function to print out the depth-first enumeration of the tree. Specify the root or sub root of the tree you wish to view.
   * 
   * @param root
   *          The root node of the tree or sub-tree.
   */
  public static void showDepthFirstTraversal(BooleanLogicTreeNode root) {
    System.out.println("DepthFirstTraversal");
    Enumeration<?> e = root.depthFirstEnumeration();
    int i = -1;
    while (e.hasMoreElements()) {
      i += 1;
      BooleanLogicTreeNode n = (BooleanLogicTreeNode) e.nextElement();
      System.out.println(i + " : " + n);
    }
  }
  
  public static void showBreadthFirstTraversal(BooleanLogicTreeNode root) {
    System.out.println("BreadthFirstTraversal");
    log.debug("BooleanLogicIterator.showBreadthFirstTraversal()");
    Enumeration<?> e = root.breadthFirstEnumeration();
    int i = -1;
    while (e.hasMoreElements()) {
      i += 1;
      BooleanLogicTreeNode n = (BooleanLogicTreeNode) e.nextElement();
      System.out.println(i + " : " + n);
      log.debug(i + " : " + n);
    }
  }
  
  private void splitLeaves(BooleanLogicTreeNode node) {
    if (log.isDebugEnabled()) {
      log.debug("BoolLogic: splitLeaves()");
    }
    positives = new PriorityQueue<BooleanLogicTreeNode>(10, new BooleanLogicTreeNodeComparator());
    // positives = new ArrayList<BooleanLogicTreeNodeJexl>();
    negatives.clear();
    
    Enumeration<?> dfe = node.depthFirstEnumeration();
    while (dfe.hasMoreElements()) {
      BooleanLogicTreeNode elem = (BooleanLogicTreeNode) dfe.nextElement();
      
      if (elem.isLeaf()) {
        if (elem.isNegated()) {
          negatives.add(elem);
        } else {
          positives.add(elem);
        }
      }
    }
  }
  
  private void reHeapPriorityQueue(BooleanLogicTreeNode node) {
    positives.clear();
    Enumeration<?> dfe = node.depthFirstEnumeration();
    BooleanLogicTreeNode elem;
    while (dfe.hasMoreElements()) {
      elem = (BooleanLogicTreeNode) dfe.nextElement();
      if (elem.isLeaf() && !elem.isNegated()) {
        positives.add(elem);
      }
    }
  }

  /* *************************************************************************
   * The iterator interface methods.
   */
  public boolean hasTop() {
    return (topKey != null);
  }
  
  public Key getTopKey() {
    if (log.isDebugEnabled()) {
      log.debug("getTopKey: " + topKey);
    }
    return topKey;
  }
  
  private void setTopKey(Key key) {
    if (this.overallRange != null && key != null) {
      if (overallRange.getEndKey() != null) { // if null end key, that means range is to the end of the tablet.
        if (!this.overallRange.contains(key)) {
          topKey = null;
          return;
        }
      }
    }
    topKey = key;
  }
  
  public Value getTopValue() {
    if (topValue == null) {
      topValue = new Value(new byte[0]);
    }
    return topValue;
  }
  
  private void resetNegatives() {
    for (BooleanLogicTreeNode neg : negatives) {
      neg.setTopKey(null);
      neg.setValid(true);
    }
  }
  
  private String getEventKeyUid(Key k) {
    if (k == null || k.getColumnFamily() == null) {
      return null;
    } else {
      return k.getColumnFamily().toString();
    }
  }
  
  private String getIndexKeyUid(Key k) {
    try {
      int idx = 0;
      String sKey = k.getColumnQualifier().toString();
      idx = sKey.indexOf("\0");
      return sKey.substring(idx + 1);
    } catch (Exception e) {
      return null;
    }
  }
  
  /*
   * Remember, the Key in the BooleanLogicTreeNode is different structurally than the Key in its sub iterator because the key BooleanLogic needs to return is an
   * event key created from the index key (which is what the sub iterators are looking at!)
   */
  private Key getOptimizedAdvanceKey() throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("getOptimizedAdvanceKey() called");
    }
    Enumeration<?> bfe = root.breadthFirstEnumeration();
    ArrayList<BooleanLogicTreeNode> bfl = new ArrayList<BooleanLogicTreeNode>();
    while (bfe.hasMoreElements()) {
      BooleanLogicTreeNode node = (BooleanLogicTreeNode) bfe.nextElement();
      if (!node.isNegated()) {
        node.setAdvanceKey(node.getTopKey());
        node.setDone(false);
        bfl.add(node);
      }
    }
    
    // walk the tree backwards
    for (int i = bfl.size() - 1; i >= 0; i--) {
      if (bfl.get(i).isLeaf() || bfl.get(i).isNegated()) {
        if (log.isDebugEnabled()) {
          log.debug("leaf, isDone?: " + bfl.get(i).isDone());
        }
        continue;
      }
      
      BooleanLogicTreeNode node = bfl.get(i);
      node.setDone(false);
      if (log.isDebugEnabled()) {
        log.debug("for loop, node: " + node + " isDone? " + node.isDone());
      }
      if (node.getType() == ParserTreeConstants.JJTANDNODE) {
        // get max
        BooleanLogicTreeNode max = null;
        Enumeration<?> children = node.children();
        boolean firstTime = true;
        while (children.hasMoreElements()) {
          BooleanLogicTreeNode child = (BooleanLogicTreeNode) children.nextElement();
          
          if (child.isNegated() || child.isChildrenAllNegated()) {
            continue;
          }
          
          // all advance keys were initially set from topkey for the leaves.
          if (child.getAdvanceKey() == null) {
            log.debug("\tchild does not advance key: " + child.printNode());
            // i'm an and, i have a child that's done, mark me as done.
            node.setDone(true);
            break;
          } else {
            log.debug("\tchild advanceKey: " + child.getAdvanceKey());
          }
          
          if (firstTime) {
            firstTime = false;
            max = child;
            if (log.isDebugEnabled()) {
              log.debug("\tAND block, first valid child: " + child);
            }
            continue;
          }
          
          log.debug("\tAND block, max: " + max);
          log.debug("\tAND block, child: " + child);
          
          // first test row
          if (max.getAdvanceKey().getRow().compareTo(child.getAdvanceKey().getRow()) < 0) {
            max = child;
            if (log.isDebugEnabled()) {
              log.debug("\tAND block, child row greater, new max.");
            }
            continue;
          }
          
          // if rows are equal, test uids
          String uid_max = getEventKeyUid(max.getAdvanceKey());
          String uid_child = getEventKeyUid(child.getAdvanceKey());
          if (log.isDebugEnabled()) {
            if (uid_max == null) {
              log.debug("\tuid_max is currently null");
            } else {
              log.debug("\tuid_max: " + uid_max);
            }
            if (uid_child == null) {
              log.debug("\tuid_child is null");
            } else {
              log.debug("\tuid_child: " + uid_child);
            }
          }
          
          if (uid_max != null && uid_child != null) {
            if (uid_max.compareTo(uid_child) < 0) {
              max = child;
            }
          } else if (uid_child == null) { // one or the other is null so we want the next row
            max = child;
            log.debug("uid_child is null, we need to grab the next row.");
            break;
          } else {
            log.debug("max is null and child is not, who should we keep? child: " + child);
            break;
          }
        } // end while
        if (log.isDebugEnabled()) {
          log.debug("attemptOptimization: AND with children, max: " + max);
        }
        if (max != null) {
          node.setAdvanceKey(max.getAdvanceKey());
        } else {
          if (log.isDebugEnabled()) {
            log.debug("AND block finished, max is null");
          }
          node.setDone(true);
        }
        
      } else if (node.getType() == ParserTreeConstants.JJTORNODE) {
        // get min
        BooleanLogicTreeNode min = null;
        Enumeration<?> children = node.children();
        boolean firstTime = true;
        int numChildren = node.getChildCount();
        int allChildrenDone = 0;
        
        while (children.hasMoreElements()) {
          BooleanLogicTreeNode child = (BooleanLogicTreeNode) children.nextElement();
          
          if (log.isDebugEnabled()) {
            log.debug("\tOR block start, child: " + child);
          }
          if (child.isNegated() || child.isChildrenAllNegated()) {
            if (log.isDebugEnabled()) {
              log.debug("\tskip negated child: " + child);
            }
            numChildren -= 1;
            continue;
          }
          if (child.isDone()) {
            if (log.isDebugEnabled()) {
              log.debug("\tchild is done: " + child);
            }
            allChildrenDone += 1;
            if (numChildren == allChildrenDone) {
              if (log.isDebugEnabled()) {
                log.debug("\tnumChildren==allChildrenDone, setDone & break");
              }
              // we're done here
              node.setDone(true);
              break;
            }
          }
          
          if (child.getAdvanceKey() == null) {
            log.debug("\tOR child doesn't have top or an AdvanceKey");
            continue;
          }
          if (firstTime) {
            if (log.isDebugEnabled()) {
              log.debug("\tOR block, first valid node, min=child: " + child + "  advanceKey: " + child.getAdvanceKey());
            }
            
            firstTime = false;
            min = child;
            continue;
          }
          if (log.isDebugEnabled()) {
            log.debug("\tOR block, min: " + min);
            log.debug("\tOR block, child: " + child);
          }
          if (min.getAdvanceKey().getRow().toString().compareTo(child.getAdvanceKey().getRow().toString()) > 0) {
            // child row is less than min, set min to child
            min = child;
            if (log.isDebugEnabled()) {
              log.debug("\tmin row was greater than child, min=child: " + min);
            }
            continue;
            
          } else if (min.getAdvanceKey().getRow().compareTo(child.getAdvanceKey().getRow()) < 0) {
            // min row is less child, skip
            if (log.isDebugEnabled()) {
              log.debug("\tmin row less than childs, keep min: " + min);
            }
            continue;
            
          } else { // they're equal, test uids
            String uid_min = getEventKeyUid(min.getAdvanceKey());
            String uid_child = getEventKeyUid(child.getAdvanceKey());
            if (log.isDebugEnabled()) {
              log.debug("\ttesting uids, uid_min: " + uid_min + "  uid_child: " + uid_child);
            }
            if (uid_min != null && uid_child != null) {
              if (uid_min.compareTo(uid_child) > 0) {
                
                min = child;
                if (log.isDebugEnabled()) {
                  log.debug("\tuid_min > uid_child, set min to child: " + min);
                }
              }
            } else if (uid_min == null) {
              if (log.isDebugEnabled()) {
                log.debug("\tuid_min is null, take childs: " + uid_child);
              }
              min = child;
            }
          }
        }// end while
        if (log.isDebugEnabled()) {
          log.debug("attemptOptimization: OR with children, min: " + min);
        }
        
        if (min != null) {
          if (log.isDebugEnabled()) {
            log.debug("OR block, min != null, advanceKey? " + min.getAdvanceKey());
          }
          node.setAdvanceKey(min.getAdvanceKey());
        } else {
          log.debug("OR block, min is null..." + min);
          node.setAdvanceKey(null);
          node.setDone(true);
        }
        
      } else if (node.getType() == ParserTreeConstants.JJTJEXLSCRIPT) { // HEAD node
        if (log.isDebugEnabled()) {
          log.debug("getOptimizedAdvanceKey, HEAD node");
        }
        BooleanLogicTreeNode child = (BooleanLogicTreeNode) node.getFirstChild();
        
        if (child.isDone()) {
          if (log.isDebugEnabled()) {
            log.debug("Head node's child is done, need to move to the next row");
          }
          Key k = child.getAdvanceKey();
          if (k == null) {
            if (log.isDebugEnabled()) {
              log.debug("HEAD node, advance key is null, try to grab next row from topKey");
            }
            if (hasTop()) {
              k = this.getTopKey();
              child.setAdvanceKey(new Key(new Text(k.getRow().toString() + "\1")));
            } else {
              return null;
            }
          } else {
            Text row = new Text(k.getRow().toString() + "\1");
            k = new Key(row);
            child.setAdvanceKey(k);
          }
          
        }
        if (log.isDebugEnabled()) {
          log.debug("advance Key: " + child.getAdvanceKey());
        }
        Key key = new Key(child.getAdvanceKey().getRow(), child.getAdvanceKey().getColumnFamily(), child.getAdvanceKey().getColumnFamily());
        return key;
        
      }// end else
    }// end for
    return null;
  }
  
  /*
   * The incoming jump key has been formatted into the structure of an index key, but the leaves are eventkeys
   */
  private boolean jump(Key jumpKey) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("JUMP!");
    }
    Enumeration<?> bfe = root.breadthFirstEnumeration();
    while (bfe.hasMoreElements()) {
      BooleanLogicTreeNode n = (BooleanLogicTreeNode) bfe.nextElement();
      n.setAdvanceKey(null);
    } // now advance all nodes to the advance key
    
    if (log.isDebugEnabled()) {
      log.debug("jump, All leaves need to advance to: " + jumpKey);
    }

    String advanceUid = getIndexKeyUid(jumpKey);
    if (log.isDebugEnabled()) {
      log.debug("advanceUid =>  " + advanceUid);
    }
    boolean ok = true;
    for (BooleanLogicTreeNode leaf : positives) {
      leaf.jump(jumpKey);
    }
    return ok;
  }
  
  @SuppressWarnings("unused")
  public void next() throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("next() method called");
    }
    boolean finished = false;
    boolean ok = true;
    if (positives.isEmpty()) {
      setTopKey(null);
      return;
    }
    
    Key previousJumpKey = null;
    while (!finished) {
      
      Key jumpKey = this.getOptimizedAdvanceKey();
      
      if (jumpKey == null) { // stop?
        if (log.isDebugEnabled()) {
          log.debug("next(), jump key is null, stopping");
        }
        setTopKey(null);
        return;
      }
      
      if (log.isDebugEnabled()) {
        if (jumpKey != null) {
          log.debug("next(), jumpKey: " + jumpKey);
        } else {
          log.debug("jumpKey is null");
        }
      }
      
      boolean same = false;
      if (jumpKey != null && topKey != null) {
        // check that the uid's are not the same
        same = getIndexKeyUid(jumpKey).equals(getEventKeyUid(topKey));
        if (log.isDebugEnabled()) {
          log.debug("jumpKeyUid: " + getIndexKeyUid(jumpKey) + "  topKeyUid: " + getEventKeyUid(topKey));
        }
      }
      
      if (log.isDebugEnabled()) {
        log.debug("previousJumpKey: " + previousJumpKey);
        log.debug("current JumpKey: " + jumpKey);
      }
      
      if (jumpKey != null && !this.overallRange.contains(jumpKey)) {
        if (log.isDebugEnabled()) {
          log.debug("jumpKey is outside of range, that means the next key is out of range, stopping");
          log.debug("jumpKey: " + jumpKey + " overallRange.endKey: " + overallRange.getEndKey());
        }
        // stop
        setTopKey(null);
        return;
      }
      
      boolean previousSame = false;
      if (previousJumpKey != null && jumpKey != null) {
        previousSame = previousJumpKey.equals(jumpKey);
      }
      // -----------------------------------
      // OPTIMIZED block
      if (jumpKey != null && !same && !previousSame && ok) {
        previousJumpKey = jumpKey;
        ok = jump(jumpKey); // attempt to jump everybody forward to this row and uid.
        // tryJump = false;
        
        // now test the tree state.
        if (testTreeState()) {
          Key tempKey = root.getTopKey();
          // it is potentially valid, now we need to seek all of the negatives
          if (!negatives.isEmpty()) {
            advanceNegatives(this.root.getTopKey());
            if (!testTreeState()) {
              continue;
            }
          }
          
          if (root.getTopKey().equals(tempKey)) {
            // it's valid set nextKey and make sure it's not the same as topKey.
            if (log.isDebugEnabled()) {
              if (this.root.hasTop()) {
                log.debug("this.root.getTopKey()->" + this.root.getTopKey());
              } else {
                log.debug("next, this.root.getTopKey() is null");
              }
              
              if (topKey != null) {
                log.debug("topKey->" + topKey);
                
              } else {
                log.debug("topKey is null");
              }
            }
            if (compare(topKey, this.root.getTopKey()) != 0) {
              // topKey = this.root.getTopKey();
              setTopKey(this.root.getTopKey());
              return;
            }
          }
        }
        
        // --------------------------------------
        // Regular next block
      } else {
        
        reHeapPriorityQueue(this.root);
        BooleanLogicTreeNode node;

        while (true) {
          node = positives.poll();
          if (!node.isDone() && node.hasTop()) {
            break;
          }
          
          if (positives.isEmpty()) {
            setTopKey(null);
            return;
          }
        }
        
        if (log.isDebugEnabled()) {
          if (jumpKey == null) {
            log.debug("no jump, jumpKey is null");
          } else if (topKey == null) {
            log.debug("no jump, jumpKey: " + jumpKey + "  topKey: null");
          } else {
            log.debug("no jump, jumpKey: " + jumpKey + "  topKey: " + topKey);
          }
          log.debug("next, (no jump) min node: " + node);
          log.debug(node);
        }
        node.next();
        resetNegatives();
        
        if (!node.hasTop()) {
          // it may be part of an or, so it could be ok.
          node.setValid(false);
          if (testTreeState()) {
            // it's valid set nextKey and make sure it's not the same as topKey.
            if (topKey.compareTo(this.root.getTopKey()) != 0) {
              // topKey = this.root.getTopKey();
              if (this.overallRange != null) {
                if (this.overallRange.contains(root.getTopKey())) {
                  setTopKey(this.root.getTopKey());
                  return;
                } else {
                  setTopKey(null);
                  finished = true;
                  return;
                }
                
              } else {
                setTopKey(this.root.getTopKey());
                return;
              }
            }
          }
        } else {
          
          if (overallRange.contains(node.getTopKey())) {
            // the node had something so push it back into priority queue
            positives.add(node);
          }
          
          // now test the tree state.
          if (testTreeState()) {
            Key tempKey = root.getTopKey();
            // it is potentially valid, now we need to seek all of the negatives
            if (!negatives.isEmpty()) {
              advanceNegatives(this.root.getTopKey());
              if (!testTreeState()) {
                continue;
              }
            }
            
            if (root.getTopKey().equals(tempKey)) {
              // it's valid set nextKey and make sure it's not the same as topKey.
              if (log.isDebugEnabled()) {
                if (this.root.hasTop()) {
                  log.debug("this.root.getTopKey()->" + this.root.getTopKey());
                } else {
                  log.debug("next, this.root.getTopKey() is null");
                }
                
                if (topKey != null) {
                  log.debug("topKey->" + topKey);
                  
                } else {
                  log.debug("topKey is null");
                }
              }
              if (compare(topKey, this.root.getTopKey()) != 0) {
                // topKey = this.root.getTopKey();
                if (this.overallRange != null) {
                  if (overallRange.contains(this.root.getTopKey())) {
                    setTopKey(this.root.getTopKey());
                    return;
                  } else {
                    topKey = null;
                    finished = true;
                    return;
                  }
                } else {
                  setTopKey(this.root.getTopKey());
                  return;
                }
              }
            }
          }
          
        }
        
        // is the priority queue empty?
        if (positives.isEmpty()) {
          finished = true;
          topKey = null;
        }
      }
    }
  }
  
  /*
   * create a range for the given row of the
   */
  private void advanceNegatives(Key k) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("advancingNegatives for Key: " + k);
    }
    Text rowID = k.getRow();
    Text colFam = k.getColumnFamily();
    
    for (BooleanLogicTreeNode neg : negatives) {
      Key startKey = new Key(rowID, neg.getFieldName(), new Text(neg.getFieldValue() + "\0" + colFam));
      Key endKey = new Key(rowID, neg.getFieldName(), new Text(neg.getFieldValue() + "\0" + colFam + "\1"));
      Range range = new Range(startKey, true, endKey, false);
      
      if (log.isDebugEnabled()) {
        log.debug("range: " + range);
      }
      neg.seek(range, EMPTY_COL_FAMS, false);
      
      if (neg.hasTop()) {
        neg.setValid(false);
      }
      if (log.isDebugEnabled()) {
        if (neg.hasTop()) {
          log.debug("neg top key: " + neg.getTopKey());
        } else {
          log.debug("neg has no top");
        }
      }
    }
  }
  
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    this.overallRange = range;
    if (log.isDebugEnabled()) {
      log.debug("seek, overallRange: " + overallRange);
    }
    // Given some criteria, advance all iterators to that position.
    // NOTE: All of our iterators exist in the leaves.
    topKey = null;
    root.setTopKey(null);
    
    // set up the range iterators for the given seek range.
    // these should exist in the positives as OR iterators, but need special setup.
    setupRangerators(range);
    
    // don't take this out, if you jump rows on the tablet you could have
    // pulled nodes out of the positives priority queue. On a call to seek
    // it is usually jumping rows, so everything needs to become possibly
    // valid again.
    reHeapPriorityQueue(this.root);
    for (BooleanLogicTreeNode node : positives) {
      node.setDone(false);
      node.seek(range, columnFamilies, inclusive);
      if (log.isDebugEnabled()) {
        String tk = "empty";
        if (node.hasTop()) {
          tk = node.getTopKey().toString();
        }
        log.debug("leaf: " + node.getContents() + " topKey: " + tk);
      }
    }
    
    // Now that all nodes have been seek'd recreate the priorityQueue to sort them properly.
    splitLeaves(this.root);
    resetNegatives();
    
    // test Tree, if it's not valid, call next
    if (testTreeState() && overallRange.contains(root.getTopKey())) {
      if (!negatives.isEmpty()) {
        // now advance negatives
        advanceNegatives(this.root.getTopKey());
        if (!testTreeState()) {
          next();
        }
      }
      
      if (log.isDebugEnabled()) {
        log.debug("overallRange " + overallRange + " topKey " + this.root.getTopKey() + " contains " + overallRange.contains(this.root.getTopKey()));
      }

      if (overallRange.contains(this.root.getTopKey()) && this.root.isValid()) {
        setTopKey(this.root.getTopKey());
      } else {
        setTopKey(null);
        return;
      }
    } else {
      // seek failed in the logic test, but there may be other possible
      // values which satisfy the logic tree. Make sure our iterators aren't
      // all null, and then call next.
      
      // if(!root.hasTop()){
      if (log.isDebugEnabled()) {
        log.debug("seek, testTreeState is false, HEAD(root) does not have top");
      }
      // check nodes in positives to see if they're all null/outside range
      // or if nothing percolated up to root yet.
      List<BooleanLogicTreeNode> removals = new ArrayList<BooleanLogicTreeNode>();
      for (BooleanLogicTreeNode node : positives) {
        if (!node.hasTop() || !overallRange.contains(node.getTopKey())) {
          removals.add(node);
        }
      }
      for (BooleanLogicTreeNode node : removals) {
        positives.remove(node);
      }
      next();
      return;
    }
  }
  
  private int compare(Key k1, Key k2) {
    if (k1 != null && k2 != null) {
      return k1.compareTo(k2);
    } else if (k1 == null && k2 == null) {
      return 0;
    } else if (k1 == null) { // in this case, null is considered bigger b/c it's closer to the end of the table.
      return 1;
    } else {
      return -1;
    }
  }
  
  private void setupRangerators(Range range) throws IOException {
    if (rangerators == null || rangerators.isEmpty()) {
      return;
    }
    for (BooleanLogicTreeNode node : rangerators) {
      Set<String> fValues = new HashSet<String>();
      OrIterator orIter = new OrIterator();
      SortedKeyValueIterator<Key,Value> siter = sourceIterator.deepCopy(env);
      // create UniqFieldNameValueIterator to find uniq field names values
      UniqFieldNameValueIterator uniq = new UniqFieldNameValueIterator(node.getFieldName(), node.getLowerBound(), node.getUpperBound());
      uniq.setSource(siter);
      uniq.seek(range, EMPTY_COL_FAMS, false);
      while (uniq.hasTop()) {
        // log.debug("uniq.top: "+uniq.getTopKey());
        Key k = uniq.getTopKey();
        keyParser.parse(k);
        String val = keyParser.getFieldValue();
        if (!fValues.contains(val)) {
          fValues.add(val);
          orIter.addTerm(siter, node.getFieldName(), new Text(val), env);
          if (log.isDebugEnabled()) {
            log.debug("setupRangerators, adding to OR:  " + node.getFieldName() + ":" + val);
          }
        } else {
          log.debug("already have this one: " + val);
        }
        uniq.next();
      }
      node.setUserObject(orIter);
    }
    
  }
  
  /* *************************************************************************
   * Inner classes
   */
  public class BooleanLogicTreeNodeComparator implements Comparator<Object> {
    
    public int compare(Object o1, Object o2) {
      BooleanLogicTreeNode n1 = (BooleanLogicTreeNode) o1;
      BooleanLogicTreeNode n2 = (BooleanLogicTreeNode) o2;
      
      Key k1 = n1.getTopKey();
      Key k2 = n2.getTopKey();
      if (log.isDebugEnabled()) {
        String t1 = "null";
        String t2 = "null";
        if (k1 != null) {
          t1 = k1.getRow().toString() + "\0" + k1.getColumnFamily().toString();
        }
        if (k2 != null) {
          t2 = k2.getRow().toString() + "\0" + k2.getColumnFamily().toString();
        }
        log.debug("BooleanLogicTreeNodeComparator   \tt1: " + t1 + "  t2: " + t2);
      }
      // return t1.compareTo(t2);
      
      if (k1 != null && k2 != null) {
        return k1.compareTo(k2);
      } else if (k1 == null && k2 == null) {
        return 0;
      } else if (k1 == null) {
        return 1;
      } else {
        return -1;
      }
      
    }
  }
  
  public IteratorOptions describeOptions() {
    return new IteratorOptions(getClass().getSimpleName(), "evaluates event objects against an expression", Collections.singletonMap(QUERY_OPTION,
        "query expression"), null);
  }
  
  public boolean validateOptions(Map<String,String> options) {
    if (!options.containsKey(QUERY_OPTION)) {
      return false;
    }
    if (!options.containsKey(FIELD_INDEX_QUERY)) {
      return false;
    }
    this.updatedQuery = options.get(FIELD_INDEX_QUERY);
    return true;
  }
}
