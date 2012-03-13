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
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.swing.tree.DefaultMutableTreeNode;

import org.apache.accumulo.examples.wikisearch.parser.QueryParser.QueryTerm;
import org.apache.accumulo.examples.wikisearch.parser.RangeCalculator.RangeBounds;
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


import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * The server-side field index queries can only support operations on indexed fields. Additionally, queries that have differing ranges (i.e. one range at the
 * fieldname level and another at the fieldValue level) are not currently supported. This class removes these conflicts from the query as well as sets proper
 * capitalization configurations etc.
 * 
 * Once the query has been modified, you can pass it to the BooleanLogicIterator on the server-side via the options map.
 * 
 */
public class FieldIndexQueryReWriter {
  
  protected static final Logger log = Logger.getLogger(FieldIndexQueryReWriter.class);
  public static final String INDEXED_TERMS_LIST = "INDEXED_TERMS_LIST"; // comma separated list of indexed terms.
  public static Set<Integer> rangeNodeSet;
  
  static {
    rangeNodeSet = new HashSet<Integer>();
    rangeNodeSet.add(ParserTreeConstants.JJTLENODE);
    rangeNodeSet.add(ParserTreeConstants.JJTLTNODE);
    rangeNodeSet.add(ParserTreeConstants.JJTGENODE);
    rangeNodeSet.add(ParserTreeConstants.JJTGTNODE);
    rangeNodeSet = Collections.unmodifiableSet(rangeNodeSet);
  }
  
  /*
   * Given a JEXL Query, rewrite it and return it.
   * 
   * 1. ParseQuery 2. Transform query 3. Refactor query 4. remove non-indexed terms a. remove any tree conflicts b. collapse any branches 7. add normalized
   * values 8. adjust for case sensitivity 9. add prefix.. but jexl chokes on null byte
   */
  public static void setLogLevel(Level lev) {
    log.setLevel(lev);
  }
  
  /**
   * 
   * @param query
   * @param options
   * @return String representation of a given query.
   * @throws ParseException
   * @throws Exception
   */
  public String removeNonIndexedTermsAndInvalidRanges(String query, Map<String,String> options) throws ParseException, Exception {
    Multimap<String,String> indexedTerms = parseIndexedTerms(options);
    RewriterTreeNode node = parseJexlQuery(query);
    if (log.isDebugEnabled()) {
      log.debug("Tree: " + node.getContents());
    }
    node = removeNonIndexedTerms(node, indexedTerms);
    node = removeTreeConflicts(node, indexedTerms);
    node = collapseBranches(node);
    node = removeNegationViolations(node);
    
    if (log.isDebugEnabled()) {
      log.debug("Tree -NonIndexed: " + node.getContents());
    }
    return rebuildQueryFromTree(node);
  }
  
  /**
   * 
   * @param query
   * @param options
   * @return String representation of a given query.
   * @throws ParseException
   * @throws Exception
   */
  public String applyNormalizedTerms(String query, Map<String,String> options) throws ParseException, Exception {
    if (log.isDebugEnabled()) {
      log.debug("applyNormalizedTerms, query: " + query);
    }
    Multimap<String,String> normalizedTerms = parseIndexedTerms(options);
    RewriterTreeNode node = parseJexlQuery(query);
    if (log.isDebugEnabled()) {
      log.debug("applyNormalizedTerms, Tree: " + node.getContents());
    }
    node = orNormalizedTerms(node, normalizedTerms);
    if (log.isDebugEnabled()) {
      log.debug("applyNormalizedTerms,Normalized: " + node.getContents());
    }
    return rebuildQueryFromTree(node);
  }
  
  /**
   * 
   * @param query
   * @param fNameUpper
   * @param fValueUpper
   * @return String representation of a given query.
   * @throws ParseException
   */
  public String applyCaseSensitivity(String query, boolean fNameUpper, boolean fValueUpper) throws ParseException {
    RewriterTreeNode node = parseJexlQuery(query);
    if (log.isDebugEnabled()) {
      log.debug("Tree: " + node.getContents());
    }
    node = applyCaseSensitivity(node, fNameUpper, fValueUpper);
    if (log.isDebugEnabled()) {
      log.debug("Case: " + node.getContents());
    }
    return rebuildQueryFromTree(node);
  }
  
  private String rebuildQueryFromTree(RewriterTreeNode node) {
    if (node.isLeaf()) {
      String fName = node.getFieldName();
      String fValue = node.getFieldValue();
      String operator = node.getOperator();
      if (node.isNegated()) {
        if (node.getType() == JexlOperatorConstants.JJTEQNODE) {
          operator = JexlOperatorConstants.getOperator(JexlOperatorConstants.JJTNENODE);
        } else if (node.getType() == JexlOperatorConstants.JJTERNODE) {
          operator = JexlOperatorConstants.getOperator(JexlOperatorConstants.JJTNRNODE);
        } else if (node.getType() == JexlOperatorConstants.JJTLTNODE) {
          operator = JexlOperatorConstants.getOperator(JexlOperatorConstants.JJTGENODE);
        } else if (node.getType() == JexlOperatorConstants.JJTLENODE) {
          operator = JexlOperatorConstants.getOperator(JexlOperatorConstants.JJTGTNODE);
        } else if (node.getType() == JexlOperatorConstants.JJTGTNODE) {
          operator = JexlOperatorConstants.getOperator(JexlOperatorConstants.JJTLENODE);
        } else if (node.getType() == JexlOperatorConstants.JJTGENODE) {
          operator = JexlOperatorConstants.getOperator(JexlOperatorConstants.JJTLTNODE);
        }
      }
      return fName + operator + "'" + fValue + "'";
    } else {
      List<String> parts = new ArrayList<String>();
      Enumeration<?> children = node.children();
      while (children.hasMoreElements()) {
        RewriterTreeNode child = (RewriterTreeNode) children.nextElement();
        parts.add(rebuildQueryFromTree(child));
      }
      if (node.getType() == ParserTreeConstants.JJTJEXLSCRIPT) {
        return org.apache.commons.lang.StringUtils.join(parts, "");
      }
      String op = " " + JexlOperatorConstants.getOperator(node.getType()) + " ";
      if (log.isDebugEnabled()) {
        log.debug("Operator: " + op);
      }
      String query = org.apache.commons.lang.StringUtils.join(parts, op);
      query = "(" + query + ")";
      return query;
    }
  }
  
  /*
   * Don't use this, Jexl currently chokes on null bytes in the query
   */
  // public String applyFieldNamePrefix(String query, String prefix) throws ParseException {
  // NuwaveTreeNode node = parseJexlQuery(query);
  // if (log.isDebugEnabled()) {
  // log.debug("Tree: " + node.getContents());
  // }
  // node = applyFieldNamePrefix(node, prefix);
  // if (log.isDebugEnabled()) {
  // log.debug("Prefix: " + node.getContents());
  // }
  // return null;
  // }
  private RewriterTreeNode parseJexlQuery(String query) throws ParseException {
    if (log.isDebugEnabled()) {
      log.debug("parseJexlQuery, query: " + query);
    }
    QueryParser parser = new QueryParser();
    parser.execute(query);
    TreeNode tree = parser.getIteratorTree();
    RewriterTreeNode root = transformTreeNode(tree);
    if (log.isDebugEnabled()) {
      log.debug("parseJexlQuery, transformedTree: " + root.getContents());
    }
    root = refactorTree(root);
    if (log.isDebugEnabled()) {
      log.debug("parseJexlQuery, refactorTree: " + root.getContents());
    }
    return root;
  }
  
  /*
     *
     */
  private RewriterTreeNode transformTreeNode(TreeNode node) throws ParseException {
    if (node.getType().equals(ASTEQNode.class) || node.getType().equals(ASTNENode.class)) {
      if (log.isDebugEnabled()) {
        log.debug("transformTreeNode, Equals Node");
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
          RewriterTreeNode child = new RewriterTreeNode(ParserTreeConstants.JJTEQNODE, fName, fValue, negated);
          return child;
        }
      }
    }
    
    if (node.getType().equals(ASTERNode.class) || node.getType().equals(ASTNRNode.class)) {
      if (log.isDebugEnabled()) {
        log.debug("transformTreeNode, Regex Node");
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
          RewriterTreeNode child = new RewriterTreeNode(ParserTreeConstants.JJTERNODE, fName, fValue, negated);
          return child;
        }
      }
    }
    
    if (node.getType().equals(ASTLTNode.class) || node.getType().equals(ASTLENode.class) || node.getType().equals(ASTGTNode.class)
        || node.getType().equals(ASTGENode.class)) {
      if (log.isDebugEnabled()) {
        log.debug("transformTreeNode, LT/LE/GT/GE node");
      }
      Multimap<String,QueryTerm> terms = node.getTerms();
      for (String fName : terms.keySet()) {
        Collection<QueryTerm> values = terms.get(fName);
        for (QueryTerm t : values) {
          if (null == t || null == t.getValue()) {
            continue;
          }
          String fValue = t.getValue().toString();
          fValue = fValue.replaceAll("'", "").toLowerCase();
          boolean negated = false; // to be negated, must be child of Not, which is handled elsewhere.
          int mytype = JexlOperatorConstants.getJJTNodeType(t.getOperator());
          RewriterTreeNode child = new RewriterTreeNode(mytype, fName, fValue, negated);
          return child;
        }
      }
    }
    
    RewriterTreeNode returnNode = null;
    
    if (node.getType().equals(ASTAndNode.class) || node.getType().equals(ASTOrNode.class)) {
      int parentType = node.getType().equals(ASTAndNode.class) ? ParserTreeConstants.JJTANDNODE : ParserTreeConstants.JJTORNODE;
      if (log.isDebugEnabled()) {
        log.debug("transformTreeNode, AND/OR node: " + parentType);
      }
      if (node.isLeaf() || !node.getTerms().isEmpty()) {
        returnNode = new RewriterTreeNode(parentType);
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
            int childType = JexlOperatorConstants.getJJTNodeType(t.getOperator());
            RewriterTreeNode child = new RewriterTreeNode(childType, fName, fValue, negated);
            if (log.isDebugEnabled()) {
              log.debug("adding child node: " + child.getContents());
            }
            returnNode.add(child);
          }
        }
      } else {
        returnNode = new RewriterTreeNode(parentType);
      }
    } else if (node.getType().equals(ASTNotNode.class)) {
      if (log.isDebugEnabled()) {
        log.debug("transformTreeNode, NOT node");
      }
      if (node.isLeaf()) {
        // NOTE: this should be cleaned up a bit.
        Multimap<String,QueryTerm> terms = node.getTerms();
        for (String fName : terms.keySet()) {
          Collection<QueryTerm> values = terms.get(fName);
          for (QueryTerm t : values) {
            if (null == t || null == t.getValue()) {
              continue;
            }
            String fValue = t.getValue().toString();
            fValue = fValue.replaceAll("'", "").toLowerCase();
            boolean negated = !t.getOperator().equals("!=");
            int mytype = JexlOperatorConstants.getJJTNodeType(t.getOperator());
            return new RewriterTreeNode(mytype, fName, fValue, negated);
          }
        }
      } else {
        returnNode = new RewriterTreeNode(ParserTreeConstants.JJTNOTNODE);
      }
    } else if (node.getType().equals(ASTJexlScript.class) || node.getType().getSimpleName().equals("RootNode")) {
      
      if (log.isDebugEnabled()) {
        log.debug("transformTreeNode, ROOT/JexlScript node");
      }
      if (node.isLeaf()) {
        returnNode = new RewriterTreeNode(ParserTreeConstants.JJTJEXLSCRIPT);
        // NOTE: this should be cleaned up a bit.
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
            int mytype = JexlOperatorConstants.getJJTNodeType(t.getOperator());
            RewriterTreeNode child = new RewriterTreeNode(mytype, fName, fValue, negated);
            returnNode.add(child);
            return returnNode;
          }
        }
      } else {
        returnNode = new RewriterTreeNode(ParserTreeConstants.JJTJEXLSCRIPT);
      }
    } else {
      log.error("transformTreeNode,  Currently Unsupported Node type: " + node.getClass().getName() + " \t" + node.getType());
    }
    for (TreeNode child : node.getChildren()) {
      returnNode.add(transformTreeNode(child));
    }
    
    return returnNode;
  }
  
  private RewriterTreeNode removeNonIndexedTerms(RewriterTreeNode root, Multimap<String,String> indexedTerms) throws Exception {
    // public void removeNonIndexedTerms(BooleanLogicTreeNodeJexl myroot, String indexedTerms) throws Exception {
    if (indexedTerms.isEmpty()) {
      throw new Exception("removeNonIndexedTerms, indexed Terms empty");
    }
    
    // NOTE: doing a depth first enumeration didn't work when I started
    // removing nodes halfway through. The following method does work,
    // it's essentially a reverse breadth first traversal.
    List<RewriterTreeNode> nodes = new ArrayList<RewriterTreeNode>();
    Enumeration<?> bfe = root.breadthFirstEnumeration();
    
    while (bfe.hasMoreElements()) {
      RewriterTreeNode node = (RewriterTreeNode) bfe.nextElement();
      nodes.add(node);
    }
    
    // walk backwards
    for (int i = nodes.size() - 1; i >= 0; i--) {
      RewriterTreeNode node = nodes.get(i);
      if (log.isDebugEnabled()) {
        log.debug("removeNonIndexedTerms, analyzing node: " + node.toString() + "  " + node.printNode());
      }
      if (node.getType() == ParserTreeConstants.JJTANDNODE || node.getType() == ParserTreeConstants.JJTORNODE) {
        // If all of your children are gone, AND/OR has no purpose, remove
        if (node.getChildCount() == 0) {
          node.removeFromParent();
          
          // If AND/OR has only 1 child, attach it to the parent directly.
        } else if (node.getChildCount() == 1) {
          RewriterTreeNode p = (RewriterTreeNode) node.getParent();
          RewriterTreeNode c = (RewriterTreeNode) node.getFirstChild();
          node.removeFromParent();
          p.add(c);
        }
      } else if (node.getType() == ParserTreeConstants.JJTJEXLSCRIPT) { // Head node
        // If head node has no children, we have nothing to search on.
        if (node.getChildCount() == 0) {
          throw new Exception();
        }
      } else if (rangeNodeSet.contains(node.getType())) { // leave it alone
        // leave ranges untouched, they'll be handled elsewhere.
        continue;
      } else {
        if (log.isDebugEnabled()) {
          log.debug("removeNonIndexedTerms, Testing: " + node.getFieldName() + ":" + node.getFieldValue());
        }
        
        if (!indexedTerms.containsKey(node.getFieldName().toString() + ":" + node.getFieldValue().toString())) {
          if (log.isDebugEnabled()) {
            log.debug(node.getFieldName() + ":" + node.getFieldValue() + " is NOT indexed");
          }
          node.removeFromParent();
        } else {
          if (log.isDebugEnabled()) {
            log.debug(node.getFieldName() + ":" + node.getFieldValue() + " is indexed");
          }
        }
      }
    }
    
    return root;
  }
  
  private RewriterTreeNode orNormalizedTerms(RewriterTreeNode myroot, Multimap<String,String> indexedTerms) throws Exception {
    // we have multimap of FieldName to multiple FieldValues
    if (indexedTerms.isEmpty()) {
      throw new Exception("indexed Terms empty");
    }
    try {
      // NOTE: doing a depth first enumeration didn't work when I started
      // removing nodes halfway through. The following method does work,
      // it's essentially a reverse breadth first traversal.
      List<RewriterTreeNode> nodes = new ArrayList<RewriterTreeNode>();
      Enumeration<?> bfe = myroot.breadthFirstEnumeration();
      
      while (bfe.hasMoreElements()) {
        RewriterTreeNode node = (RewriterTreeNode) bfe.nextElement();
        nodes.add(node);
      }
      
      // walk backwards
      for (int i = nodes.size() - 1; i >= 0; i--) {
        RewriterTreeNode node = nodes.get(i);
        if (log.isDebugEnabled()) {
          log.debug("orNormalizedTerms, analyzing node: " + node.toString() + "  " + node.printNode());
        }
        if (node.getType() == ParserTreeConstants.JJTANDNODE || node.getType() == ParserTreeConstants.JJTORNODE) {
          continue;
        } else if (node.getType() == ParserTreeConstants.JJTJEXLSCRIPT) {
          if (node.getChildCount() == 0) {
            if (log.isDebugEnabled()) {
              log.debug("orNormalizedTerms: Head node has no children!");
            }
            throw new Exception(); // Head node has no children.
          }
        } else {
          if (log.isDebugEnabled()) {
            log.debug("Testing data location: " + node.getFieldName());
          }
          String fName = node.getFieldName().toString();
          String fValue = node.getFieldValue().toString();
          if (indexedTerms.containsKey(fName + ":" + fValue)) {
            
            if (indexedTerms.get(fName + ":" + fValue).size() > 1) {
              // Replace node with an OR, and make children from the multimap collection
              node.setType(ParserTreeConstants.JJTORNODE);
              boolean neg = node.isNegated();
              node.setNegated(false);
              node.setFieldName(null);
              node.setFieldValue(null);
              Collection<String> values = indexedTerms.get(fName + ":" + fValue);
              for (String value : values) {
                RewriterTreeNode n = new RewriterTreeNode(ParserTreeConstants.JJTEQNODE, fName, value, neg);
                node.add(n);
              }
            } else if (indexedTerms.get(fName + ":" + fValue).size() == 1) {
              // Straight replace
              Collection<String> values = indexedTerms.get(fName + ":" + fValue);
              for (String val : values) {
                // should only be 1
                node.setFieldValue(val);
              }
            }
            
          } else {
            // throw new Exception("orNormalizedTerms, encountered a non-indexed term: " + node.getFieldName().toString());
          }
        }
      }
    } catch (Exception e) {
      log.debug("Caught exception in orNormalizedTerms(): " + e);
      throw new Exception("exception in: orNormalizedTerms");
    }
    
    return myroot;
  }
  
  /***
   * We only want to pass ranges on if they meet a very narrow set of conditions. All ranges must be bounded i.e. x between(1,5) so their parent is an AND. We
   * will only pass a range if 1. The AND is the direct child of HEAD node 2. The AND is a child of an OR which is a direct child of HEAD node.
   * 
   * If there is an HEAD-AND[x,OR[b,AND[range]]], and you remove the range, this turns the tree into HEAD-AND[X,OR[B]] which becomes HEAD-AND[X,B] which will
   * miss entries, so you need to cut out the entire OR at this point and let the positive side of the AND pick it up.
   */
  private RewriterTreeNode removeTreeConflicts(RewriterTreeNode root, Multimap<String,String> indexedTerms) {
    if (log.isDebugEnabled()) {
      log.debug("removeTreeConflicts");
    }
    
    /*
     * You can't modify the enumeration, so save it into a list. We want to walk backwards in a breadthFirstEnumeration. So we don't throw null pointers when we
     * erase nodes and shorten our list.
     */
    List<RewriterTreeNode> nodeList = new ArrayList<RewriterTreeNode>();
    Enumeration<?> nodes = root.breadthFirstEnumeration();
    while (nodes.hasMoreElements()) {
      RewriterTreeNode child = (RewriterTreeNode) nodes.nextElement();
      nodeList.add(child);
    }
    
    // walk backwards
    for (int i = nodeList.size() - 1; i >= 0; i--) {
      RewriterTreeNode node = nodeList.get(i);
      
      if (node.isRemoval()) {
        node.removeFromParent();
        continue;
      }
      
      RewriterTreeNode parent = (RewriterTreeNode) node.getParent();
      /*
       * All ranges must be bounded! This means the range must be part of an AND, and the parent of AND must be a HEAD node or an OR whose parent is a HEAD
       * node.
       */
      if (node.getType() == ParserTreeConstants.JJTANDNODE
          && (node.getLevel() == 1 || (parent.getType() == ParserTreeConstants.JJTORNODE && parent.getLevel() == 1))) {
        
        if (log.isDebugEnabled()) {
          log.debug("AND at level 1 or with OR parent at level 1");
        }
        Map<Text,RangeBounds> rangeMap = getBoundedRangeMap(node);
        
        // can't modify the enumeration... save children to a list.
        List<RewriterTreeNode> childList = new ArrayList<RewriterTreeNode>();
        Enumeration<?> children = node.children();
        while (children.hasMoreElements()) {
          RewriterTreeNode child = (RewriterTreeNode) children.nextElement();
          childList.add(child);
        }
        
        for (int j = childList.size() - 1; j >= 0; j--) {
          RewriterTreeNode child = childList.get(j);
          // currently we are not allowing unbounded ranges, so they must sit under an AND node.
          if (rangeNodeSet.contains(child.getType())) {
            if (log.isDebugEnabled()) {
              log.debug("child type: " + JexlOperatorConstants.getOperator(child.getType()));
            }
            if (rangeMap == null) {
              // remove
              child.removeFromParent();
            } else {
              if (!rangeMap.containsKey(new Text(child.getFieldName()))) {
                child.removeFromParent();
              } else {
                // check if it has a single non-range sibling
                boolean singleSib = false;
                if (log.isDebugEnabled()) {
                  log.debug("checking for singleSib.");
                }
                Enumeration<?> sibs = child.getParent().children();
                while (sibs.hasMoreElements()) {
                  RewriterTreeNode sib = (RewriterTreeNode) sibs.nextElement();
                  if (!rangeNodeSet.contains(sib.getType())) {
                    singleSib = true;
                    break;
                  }
                }
                if (singleSib) {
                  child.removeFromParent();
                } else {
                  if (indexedTerms.containsKey(child.getFieldName() + ":" + child.getFieldValue())) {
                    if (log.isDebugEnabled()) {
                      log.debug("removeTreeConflicts, node: " + node.getContents());
                    }
                    // swap parent AND with an OR
                    node.removeAllChildren();
                    node.setType(ParserTreeConstants.JJTORNODE);
                    
                    Collection<String> values = indexedTerms.get(child.getFieldName() + ":" + child.getFieldValue());
                    for (String value : values) {
                      RewriterTreeNode n = new RewriterTreeNode(ParserTreeConstants.JJTEQNODE, child.getFieldName(), value, child.isNegated());
                      node.add(n);
                    }
                    if (log.isDebugEnabled()) {
                      log.debug("removeTreeConflicts, node: " + node.getContents());
                    }
                    
                    break;
                  } else {
                    child.removeFromParent();
                  }
                  
                }
              }
            }
          }
        }// end inner for
        
      } else { // remove all ranges!
        if (node.isLeaf()) {
          continue;
        }
        // can't modify the enumeration...
        List<RewriterTreeNode> childList = new ArrayList<RewriterTreeNode>();
        Enumeration<?> children = node.children();
        while (children.hasMoreElements()) {
          RewriterTreeNode child = (RewriterTreeNode) children.nextElement();
          childList.add(child);
        }
        
        // walk backwards
        for (int j = childList.size() - 1; j >= 0; j--) {
          
          RewriterTreeNode child = childList.get(j);
          if (log.isDebugEnabled()) {
            log.debug("removeTreeConflicts, looking at node: " + node);
          }
          if (rangeNodeSet.contains(child.getType())) {
            // if grand parent is an OR and not top level, mark whole thing for removal.
            RewriterTreeNode grandParent = (RewriterTreeNode) child.getParent().getParent();
            if (grandParent.getType() == ParserTreeConstants.JJTORNODE && grandParent.getLevel() != 1) {
              grandParent.setRemoval(true);
            }
            child.removeFromParent();
          }
        }
      }
      
    }// end outer for
    
    return root;
  }
  
  private RewriterTreeNode removeNegationViolations(RewriterTreeNode node) throws Exception {
    // Double check the top level node for negation violations
    // if AND, one child must be positive, if OR, no negatives allowed.
    RewriterTreeNode one = (RewriterTreeNode) node.getFirstChild(); // Head node has only 1 child.
    ArrayList<RewriterTreeNode> childrenList = new ArrayList<RewriterTreeNode>();
    Enumeration<?> children = one.children();
    while (children.hasMoreElements()) {
      RewriterTreeNode child = (RewriterTreeNode) children.nextElement();
      childrenList.add(child);
    }
    if (one.getType() == JexlOperatorConstants.JJTORNODE) {
      for (RewriterTreeNode child : childrenList) {
        if (child.isNegated()) {
          child.removeFromParent();
        }
      }
      if (one.getChildCount() == 0) {
        throw new Exception("FieldIndexQueryReWriter: Top level query node cannot be processed.");
      }
    } else if (one.getType() == JexlOperatorConstants.JJTANDNODE) {
      boolean ok = false;
      for (RewriterTreeNode child : childrenList) {
        if (!child.isNegated()) {
          ok = true;
          break;
        }
      }
      if (!ok) {
        throw new Exception("FieldIndexQueryReWriter: Top level query node cannot be processed.");
      }
    }
    
    return node;
  }
  
  // After tree conflicts have been resolve, we can collapse branches where
  // leaves have been pruned.
  private RewriterTreeNode collapseBranches(RewriterTreeNode myroot) throws Exception {
    
    // NOTE: doing a depth first enumeration didn't wory when I started
    // removing nodes halfway through. The following method does work,
    // it's essentially a reverse breadth first traversal.
    List<RewriterTreeNode> nodes = new ArrayList<RewriterTreeNode>();
    Enumeration<?> bfe = myroot.breadthFirstEnumeration();
    
    while (bfe.hasMoreElements()) {
      RewriterTreeNode node = (RewriterTreeNode) bfe.nextElement();
      nodes.add(node);
    }
    
    // walk backwards
    for (int i = nodes.size() - 1; i >= 0; i--) {
      RewriterTreeNode node = nodes.get(i);
      if (log.isDebugEnabled()) {
        log.debug("collapseBranches, inspecting node: " + node.toString() + "  " + node.printNode());
      }
      
      if (node.getType() == ParserTreeConstants.JJTANDNODE || node.getType() == ParserTreeConstants.JJTORNODE) {
        if (node.getChildCount() == 0) {
          node.removeFromParent();
        } else if (node.getChildCount() == 1) {
          RewriterTreeNode p = (RewriterTreeNode) node.getParent();
          RewriterTreeNode c = (RewriterTreeNode) node.getFirstChild();
          node.removeFromParent();
          p.add(c);
          
        }
      } else if (node.getType() == ParserTreeConstants.JJTJEXLSCRIPT) {
        if (node.getChildCount() == 0) {
          throw new Exception();
        }
      }
    }
    return myroot;
  }
  
  /**
   * @param options
   */
  public Multimap<String,String> parseIndexedTerms(Map<String,String> options) {
    if (options.get(INDEXED_TERMS_LIST) != null) {
      Multimap<String,String> mmap = HashMultimap.create();
      String[] items = options.get(INDEXED_TERMS_LIST).split(";");
      for (String item : items) {
        item = item.trim();
        if (log.isDebugEnabled()) {}
        String[] parts = item.split(":");
        if (log.isDebugEnabled()) {
          log.debug("adding: " + parts[0]);
        }
        for (int i = 2; i < parts.length; i++) {
          // key is original query token, i.e. color:red
          mmap.put(parts[0] + ":" + parts[1], parts[i]);
          
        }
        
      }
      if (log.isDebugEnabled()) {
        log.debug("multimap: " + mmap);
      }
      return mmap;
    }
    if (log.isDebugEnabled()) {
      log.debug("parseIndexedTerms: returning null");
    }
    return null;
  }
  
  /**
   * @param root
   */
  public RewriterTreeNode refactorTree(RewriterTreeNode root) {
    Enumeration<?> dfe = root.breadthFirstEnumeration();
    
    while (dfe.hasMoreElements()) {
      RewriterTreeNode n = (RewriterTreeNode) dfe.nextElement();
      
      if (n.getType() == ParserTreeConstants.JJTNOTNODE) {// BooleanLogicTreeNode.NodeType.NOT) {
        RewriterTreeNode child = (RewriterTreeNode) n.getChildAt(0);
        child.setNegated(true);
        RewriterTreeNode parent = (RewriterTreeNode) n.getParent();
        parent.remove(n);
        parent.add(child);
      }
    }
    
    // cycle through again and distribute nots
    Enumeration<?> bfe = root.breadthFirstEnumeration();
    RewriterTreeNode child;
    
    while (bfe.hasMoreElements()) {
      child = (RewriterTreeNode) bfe.nextElement();
      
      if (child.isNegated()) {
        if (child.getChildCount() > 0) {
          demorganSubTree(child);
          
        }
      }
    }
    return root;
    
  }
  
  private void demorganSubTree(RewriterTreeNode root) {
    
    root.setNegated(false);
    // root.setChildrenAllNegated(true);
    
    if (root.getType() == ParserTreeConstants.JJTANDNODE) {// BooleanLogicTreeNode.NodeType.AND) {
      // root.setType(BooleanLogicTreeNode.NodeType.OR);
      root.setType(ParserTreeConstants.JJTORNODE);
    } else if (root.getType() == ParserTreeConstants.JJTORNODE) {// BooleanLogicTreeNode.NodeType.OR) {
      // root.setType(BooleanLogicTreeNode.NodeType.AND);
      root.setType(ParserTreeConstants.JJTANDNODE);
    } else if (root.getType() == ParserTreeConstants.JJTEQNODE || root.getType() == ParserTreeConstants.JJTERNODE) {
      // do nothing
    } else {
      log.error("refactorSubTree, node type not supported");
    }
    
    Enumeration<?> children = root.children();
    RewriterTreeNode child = null;
    // now distribute the negative
    
    while (children.hasMoreElements()) {
      child = (RewriterTreeNode) children.nextElement();
      if (child.isNegated()) {
        child.setNegated(false);
      } else {
        child.setNegated(true);
      }
    }
  }
  
  private RewriterTreeNode applyCaseSensitivity(RewriterTreeNode root, boolean fnUpper, boolean fvUpper) {
    // for each leaf, apply case sensitivity
    Enumeration<?> bfe = root.breadthFirstEnumeration();
    while (bfe.hasMoreElements()) {
      RewriterTreeNode node = (RewriterTreeNode) bfe.nextElement();
      if (node.isLeaf()) {
        String fName = fnUpper ? node.getFieldName().toUpperCase() : node.getFieldName().toLowerCase();
        node.setFieldName(fName);
        
        String fValue = fvUpper ? node.getFieldValue().toUpperCase() : node.getFieldValue().toLowerCase();
        node.setFieldValue(fValue);
        
      }
    }
    return root;
  }
  
  private Map<Text,RangeBounds> getBoundedRangeMap(RewriterTreeNode node) {
    
    if (node.getType() == ParserTreeConstants.JJTANDNODE || node.getType() == ParserTreeConstants.JJTORNODE) {
      Enumeration<?> children = node.children();
      Map<Text,RangeBounds> rangeMap = new HashMap<Text,RangeBounds>();
      while (children.hasMoreElements()) {
        RewriterTreeNode child = (RewriterTreeNode) children.nextElement();
        if (child.getType() == ParserTreeConstants.JJTLENODE || child.getType() == ParserTreeConstants.JJTLTNODE) {
          Text fName = new Text(child.getFieldName());
          if (rangeMap.containsKey(fName)) {
            RangeBounds rb = rangeMap.get(fName);
            if (rb.getLower() != null) {
              log.error("testBoundedRangeExistence, two lower bounds exist for bounded range.");
            }
            rb.setLower(new Text(child.getFieldValue()));
          } else {
            RangeBounds rb = new RangeBounds();
            rb.setLower(new Text(child.getFieldValue()));
            rangeMap.put(new Text(child.getFieldName()), rb);
          }
          
        } else if (child.getType() == ParserTreeConstants.JJTGENODE || child.getType() == ParserTreeConstants.JJTGTNODE) {
          Text fName = new Text(child.getFieldName());
          if (rangeMap.containsKey(fName)) {
            RangeBounds rb = rangeMap.get(fName);
            if (rb.getUpper() != null) {
              log.error("testBoundedRangeExistence, two Upper bounds exist for bounded range.");
            }
            rb.setUpper(new Text(child.getFieldValue()));
          } else {
            RangeBounds rb = new RangeBounds();
            rb.setUpper(new Text(child.getFieldValue()));
            rangeMap.put(new Text(child.getFieldName()), rb);
          }
        }
      }
      
      for (Entry<Text,RangeBounds> entry : rangeMap.entrySet()) {
        RangeBounds rb = entry.getValue();
        if (rb.getLower() == null || rb.getUpper() == null) {
          // unbounded range, remove
          if (log.isDebugEnabled()) {
            log.debug("testBoundedRangeExistence: Unbounded Range detected, removing entry from rangeMap");
          }
          rangeMap.remove(entry.getKey());
        }
      }
      if (!rangeMap.isEmpty()) {
        return rangeMap;
      }
    }
    
    return null;
  }
  
  /**
   * INNER CLASSES
   */
  public class RewriterTreeNode extends DefaultMutableTreeNode {
    
    private static final long serialVersionUID = 1L;
    private boolean negated = false;
    private String fieldName;
    private String fieldValue;
    private String operator;
    private int type;
    private boolean removal = false;
    
    /**
     * 
     * @param type
     */
    public RewriterTreeNode(int type) {
      super();
      this.type = type;
    }
    
    /**
     * 
     * @param type
     * @param fName
     * @param fValue
     */
    public RewriterTreeNode(int type, String fName, String fValue) {
      super();
      init(type, fName, fValue);
    }
    
    /**
     * 
     * @param type
     * @param fName
     * @param fValue
     * @param negate
     */
    public RewriterTreeNode(int type, String fName, String fValue, boolean negate) {
      super();
      init(type, fName, fValue, negate);
    }
    
    private void init(int type, String fName, String fValue) {
      init(type, fName, fValue, false);
    }
    
    private void init(int type, String fName, String fValue, boolean negate) {
      this.type = type;
      this.fieldName = fName;
      this.fieldValue = fValue;
      this.negated = negate;
      this.operator = JexlOperatorConstants.getOperator(type);
      if (log.isDebugEnabled()) {
        log.debug("FN: " + this.fieldName + "  FV: " + this.fieldValue + " Op: " + this.operator);
      }
    }
    
    /**
     * @return The field name.
     */
    public String getFieldName() {
      return fieldName;
    }
    
    /**
     * 
     * @param fieldName
     */
    public void setFieldName(String fieldName) {
      this.fieldName = fieldName;
    }
    
    /**
     * 
     * @return The field value.
     */
    public String getFieldValue() {
      return fieldValue;
    }
    
    /**
     * 
     * @param fieldValue
     */
    public void setFieldValue(String fieldValue) {
      this.fieldValue = fieldValue;
    }
    
    /**
     * 
     * @return true if negated, otherwise false.
     */
    public boolean isNegated() {
      return negated;
    }
    
    /**
     * 
     * @param negated
     */
    public void setNegated(boolean negated) {
      this.negated = negated;
    }
    
    /**
     * 
     * @return The operator.
     */
    public String getOperator() {
      return operator;
    }
    
    /**
     * 
     * @param operator
     */
    public void setOperator(String operator) {
      this.operator = operator;
    }
    
    /**
     * 
     * @return The type.
     */
    public int getType() {
      return type;
    }
    
    /**
     * 
     * @param type
     */
    public void setType(int type) {
      this.type = type;
    }
    
    public boolean isRemoval() {
      return removal;
    }
    
    public void setRemoval(boolean removal) {
      this.removal = removal;
    }
    
    public String getContents() {
      StringBuilder s = new StringBuilder("[");
      s.append(toString());
      
      if (children != null) {
        Enumeration<?> e = this.children();
        while (e.hasMoreElements()) {
          RewriterTreeNode n = (RewriterTreeNode) e.nextElement();
          s.append(",");
          s.append(n.getContents());
        }
      }
      s.append("]");
      return s.toString();
    }
    
    /**
     * 
     * @return A string represenation of the field name and value.
     */
    public String printNode() {
      StringBuilder s = new StringBuilder("[");
      s.append("Full Location & Term = ");
      if (this.fieldName != null) {
        s.append(this.fieldName.toString());
      } else {
        s.append("BlankDataLocation");
      }
      s.append("  ");
      if (this.fieldValue != null) {
        s.append(this.fieldValue.toString());
      } else {
        s.append("BlankTerm");
      }
      s.append("]");
      return s.toString();
    }
    
    @Override
    public String toString() {
      switch (type) {
        case ParserTreeConstants.JJTEQNODE:
          return fieldName + ":" + fieldValue + ":negated=" + isNegated();
        case ParserTreeConstants.JJTNENODE:
          return fieldName + ":" + fieldValue + ":negated=" + isNegated();
        case ParserTreeConstants.JJTERNODE:
          return fieldName + ":" + fieldValue + ":negated=" + isNegated();
        case ParserTreeConstants.JJTNRNODE:
          return fieldName + ":" + fieldValue + ":negated=" + isNegated();
        case ParserTreeConstants.JJTLENODE:
          return fieldName + ":" + fieldValue + ":negated=" + isNegated();
        case ParserTreeConstants.JJTLTNODE:
          return fieldName + ":" + fieldValue + ":negated=" + isNegated();
        case ParserTreeConstants.JJTGENODE:
          return fieldName + ":" + fieldValue + ":negated=" + isNegated();
        case ParserTreeConstants.JJTGTNODE:
          return fieldName + ":" + fieldValue + ":negated=" + isNegated();
        case ParserTreeConstants.JJTJEXLSCRIPT:
          return "HEAD";
        case ParserTreeConstants.JJTANDNODE:
          return "AND";
        case ParserTreeConstants.JJTNOTNODE:
          return "NOT";
        case ParserTreeConstants.JJTORNODE:
          return "OR";
        default:
          System.out.println("Problem in NuwaveTreeNode.toString()");
          return null;
      }
    }
  }
}
