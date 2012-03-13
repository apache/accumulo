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

import java.io.StringReader;

import org.apache.accumulo.examples.wikisearch.parser.QueryParser.EvaluationContext;
import org.apache.accumulo.examples.wikisearch.parser.QueryParser.FunctionResult;
import org.apache.accumulo.examples.wikisearch.parser.QueryParser.LiteralResult;
import org.apache.accumulo.examples.wikisearch.parser.QueryParser.ObjectHolder;
import org.apache.accumulo.examples.wikisearch.parser.QueryParser.QueryTerm;
import org.apache.accumulo.examples.wikisearch.parser.QueryParser.TermResult;
import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAmbiguous;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTArrayAccess;
import org.apache.commons.jexl2.parser.ASTArrayLiteral;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl2.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl2.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl2.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl2.parser.ASTBlock;
import org.apache.commons.jexl2.parser.ASTConstructorNode;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFloatLiteral;
import org.apache.commons.jexl2.parser.ASTForeachStatement;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTIntegerLiteral;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMapEntry;
import org.apache.commons.jexl2.parser.ASTMapLiteral;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTModNode;
import org.apache.commons.jexl2.parser.ASTMulNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTSizeFunction;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTernaryNode;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.ASTWhileStatement;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.Parser;
import org.apache.commons.jexl2.parser.ParserVisitor;
import org.apache.commons.jexl2.parser.SimpleNode;


import com.google.common.collect.Multimap;

/**
 * Class that parses the query and returns a tree of TreeNode's. This class rolls up clauses that are below like conjunctions (AND, OR) for the purposes of
 * creating intersecting iterators.
 * 
 */
public class TreeBuilder implements ParserVisitor {
  
  class RootNode extends JexlNode {
    
    public RootNode(int id) {
      super(id);
    }
    
    public RootNode(Parser p, int id) {
      super(p, id);
    }
    
  }
  
  private TreeNode rootNode = null;
  private TreeNode currentNode = null;
  private boolean currentlyInCheckChildren = false;
  
  public TreeBuilder(String query) throws ParseException {
    Parser p = new Parser(new StringReader(";"));
    ASTJexlScript script = p.parse(new StringReader(query), null);
    // Check to see if the child node is an AND or OR. If not, then
    // there must be just a single value in the query expression
    rootNode = new TreeNode();
    rootNode.setType(RootNode.class);
    currentNode = rootNode;
    EvaluationContext ctx = new EvaluationContext();
    script.childrenAccept(this, ctx);
  }
  
  public TreeBuilder(ASTJexlScript script) {
    // Check to see if the child node is an AND or OR. If not, then
    // there must be just a single value in the query expression
    rootNode = new TreeNode();
    rootNode.setType(RootNode.class);
    currentNode = rootNode;
    EvaluationContext ctx = new EvaluationContext();
    script.childrenAccept(this, ctx);
  }
  
  public TreeNode getRootNode() {
    return this.rootNode;
  }
  
  public Object visit(SimpleNode node, Object data) {
    return null;
  }
  
  public Object visit(ASTJexlScript node, Object data) {
    return null;
  }
  
  public Object visit(ASTBlock node, Object data) {
    return null;
  }
  
  public Object visit(ASTAmbiguous node, Object data) {
    return null;
  }
  
  public Object visit(ASTIfStatement node, Object data) {
    return null;
  }
  
  public Object visit(ASTWhileStatement node, Object data) {
    return null;
  }
  
  public Object visit(ASTForeachStatement node, Object data) {
    return null;
  }
  
  public Object visit(ASTAssignment node, Object data) {
    return null;
  }
  
  public Object visit(ASTTernaryNode node, Object data) {
    return null;
  }
  
  /**
   * @param node
   * @param failClass
   * @return false if any of the nodes equals the fail class or contain a NOT in the subtree
   */
  private boolean nodeCheck(JexlNode node, Class<?> failClass) {
    if (node.getClass().equals(failClass) || node.getClass().equals(ASTNotNode.class))
      return false;
    else {
      for (int i = 0; i < node.jjtGetNumChildren(); i++) {
        if (!nodeCheck(node.jjtGetChild(i), failClass))
          return false;
      }
    }
    return true;
  }
  
  /**
   * Checks to see if all of the child nodes are of the same type (AND/OR) and if so then aggregates all of the child terms. If not returns null.
   * 
   * @param parent
   * @param parentNode
   * @return Map of field names to query terms or null
   */
  private Multimap<String,QueryTerm> checkChildren(JexlNode parent, EvaluationContext ctx) {
    // If the current node is an AND, then make sure that there is no
    // OR descendant node, and vice versa. If this is true, then we call
    // roll up all of the descendent values.
    this.currentlyInCheckChildren = true;
    Multimap<String,QueryTerm> rolledUpTerms = null;
    boolean result = false;
    if (parent.getClass().equals(ASTOrNode.class)) {
      for (int i = 0; i < parent.jjtGetNumChildren(); i++) {
        result = nodeCheck(parent.jjtGetChild(i), ASTAndNode.class);
        if (!result)
          break;
      }
    } else {
      for (int i = 0; i < parent.jjtGetNumChildren(); i++) {
        result = nodeCheck(parent.jjtGetChild(i), ASTOrNode.class);
        if (!result)
          break;
      }
    }
    if (result) {
      // Set current node to a fake node and
      // roll up the children from this node using the visitor pattern.
      TreeNode rollupFakeNode = new TreeNode();
      TreeNode previous = this.currentNode;
      this.currentNode = rollupFakeNode;
      // Run the visitor with the fake node.
      parent.childrenAccept(this, ctx);
      // Get the terms from the fake node
      rolledUpTerms = this.currentNode.getTerms();
      // Reset the current node pointer
      this.currentNode = previous;
    }
    this.currentlyInCheckChildren = false;
    return rolledUpTerms;
  }
  
  public Object visit(ASTOrNode node, Object data) {
    boolean previouslyInOrContext = false;
    EvaluationContext ctx = null;
    if (null != data && data instanceof EvaluationContext) {
      ctx = (EvaluationContext) data;
      previouslyInOrContext = ctx.inOrContext;
    } else {
      ctx = new EvaluationContext();
    }
    ctx.inOrContext = true;
    // Are we being called from the checkChildren method? If so, then we
    // are rolling up terms. If not, then we need to call check children.
    if (currentlyInCheckChildren) {
      // Process both sides of this node.
      node.jjtGetChild(0).jjtAccept(this, data);
      node.jjtGetChild(1).jjtAccept(this, data);
    } else {
      // Create a new OR node under the current node.
      TreeNode orNode = new TreeNode();
      orNode.setType(ASTOrNode.class);
      orNode.setParent(this.currentNode);
      this.currentNode.getChildren().add(orNode);
      Multimap<String,QueryTerm> terms = checkChildren(node, ctx);
      if (terms == null) {
        // Then there was no rollup, set the current node to the orNode
        // and process the children. Be sure to set the current Node to
        // the or node in between calls because we could be processing
        // an AND node below and the current node will have been switched.
        // Process both sides of this node.
        currentNode = orNode;
        node.jjtGetChild(0).jjtAccept(this, data);
        currentNode = orNode;
        node.jjtGetChild(1).jjtAccept(this, data);
      } else {
        // There was a rollup, don't process the children and set the terms
        // on the or node.
        orNode.setTerms(terms);
      }
    }
    // reset the state
    if (null != data && !previouslyInOrContext)
      ctx.inOrContext = false;
    return null;
  }
  
  public Object visit(ASTAndNode node, Object data) {
    boolean previouslyInAndContext = false;
    EvaluationContext ctx = null;
    if (null != data && data instanceof EvaluationContext) {
      ctx = (EvaluationContext) data;
      previouslyInAndContext = ctx.inAndContext;
    } else {
      ctx = new EvaluationContext();
    }
    ctx.inAndContext = true;
    // Are we being called from the checkChildren method? If so, then we
    // are rolling up terms. If not, then we need to call check children.
    if (currentlyInCheckChildren) {
      // Process both sides of this node.
      node.jjtGetChild(0).jjtAccept(this, data);
      node.jjtGetChild(1).jjtAccept(this, data);
    } else {
      // Create a new And node under the current node.
      TreeNode andNode = new TreeNode();
      andNode.setType(ASTAndNode.class);
      andNode.setParent(this.currentNode);
      this.currentNode.getChildren().add(andNode);
      Multimap<String,QueryTerm> terms = checkChildren(node, ctx);
      if (terms == null) {
        // Then there was no rollup, set the current node to the orNode
        // and process the children. Be sure to set the current Node to
        // the and node in between calls because we could be processing
        // an OR node below and the current node will have been switched.
        // Process both sides of this node.
        currentNode = andNode;
        node.jjtGetChild(0).jjtAccept(this, data);
        currentNode = andNode;
        node.jjtGetChild(1).jjtAccept(this, data);
      } else {
        // There was a rollup, don't process the children and set the terms
        // on the or node.
        andNode.setTerms(terms);
      }
    }
    if (null != data && !previouslyInAndContext)
      ctx.inAndContext = false;
    return null;
  }
  
  public Object visit(ASTBitwiseOrNode node, Object data) {
    return null;
  }
  
  public Object visit(ASTBitwiseXorNode node, Object data) {
    return null;
  }
  
  public Object visit(ASTBitwiseAndNode node, Object data) {
    return null;
  }
  
  public Object visit(ASTEQNode node, Object data) {
    StringBuilder fieldName = new StringBuilder();
    ObjectHolder value = new ObjectHolder();
    // Process both sides of this node.
    Object left = node.jjtGetChild(0).jjtAccept(this, data);
    Object right = node.jjtGetChild(1).jjtAccept(this, data);
    // Ignore functions in the query
    if (left instanceof FunctionResult || right instanceof FunctionResult)
      return null;
    decodeResults(left, right, fieldName, value);
    // We need to check to see if we are in a NOT context. If so,
    // then we need to reverse the negation.
    boolean negated = false;
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      if (ctx.inNotContext)
        negated = !negated;
    }
    QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
    this.currentNode.getTerms().put(fieldName.toString(), term);
    return null;
  }
  
  public Object visit(ASTNENode node, Object data) {
    StringBuilder fieldName = new StringBuilder();
    ObjectHolder value = new ObjectHolder();
    // Process both sides of this node.
    Object left = node.jjtGetChild(0).jjtAccept(this, data);
    Object right = node.jjtGetChild(1).jjtAccept(this, data);
    // Ignore functions in the query
    if (left instanceof FunctionResult || right instanceof FunctionResult)
      return null;
    decodeResults(left, right, fieldName, value);
    // We need to check to see if we are in a NOT context. If so,
    // then we need to reverse the negation.
    boolean negated = true;
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      if (ctx.inNotContext)
        negated = !negated;
    }
    QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
    this.currentNode.getTerms().put(fieldName.toString(), term);
    return null;
  }
  
  public Object visit(ASTLTNode node, Object data) {
    StringBuilder fieldName = new StringBuilder();
    ObjectHolder value = new ObjectHolder();
    // Process both sides of this node.
    Object left = node.jjtGetChild(0).jjtAccept(this, data);
    Object right = node.jjtGetChild(1).jjtAccept(this, data);
    // Ignore functions in the query
    if (left instanceof FunctionResult || right instanceof FunctionResult)
      return null;
    decodeResults(left, right, fieldName, value);
    // We need to check to see if we are in a NOT context. If so,
    // then we need to reverse the negation.
    boolean negated = false;
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      if (ctx.inNotContext)
        negated = !negated;
    }
    QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
    this.currentNode.getTerms().put(fieldName.toString(), term);
    return null;
  }
  
  public Object visit(ASTGTNode node, Object data) {
    StringBuilder fieldName = new StringBuilder();
    ObjectHolder value = new ObjectHolder();
    // Process both sides of this node.
    Object left = node.jjtGetChild(0).jjtAccept(this, data);
    Object right = node.jjtGetChild(1).jjtAccept(this, data);
    // Ignore functions in the query
    if (left instanceof FunctionResult || right instanceof FunctionResult)
      return null;
    decodeResults(left, right, fieldName, value);
    // We need to check to see if we are in a NOT context. If so,
    // then we need to reverse the negation.
    boolean negated = false;
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      if (ctx.inNotContext)
        negated = !negated;
    }
    QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
    this.currentNode.getTerms().put(fieldName.toString(), term);
    return null;
  }
  
  public Object visit(ASTLENode node, Object data) {
    StringBuilder fieldName = new StringBuilder();
    ObjectHolder value = new ObjectHolder();
    // Process both sides of this node.
    Object left = node.jjtGetChild(0).jjtAccept(this, data);
    Object right = node.jjtGetChild(1).jjtAccept(this, data);
    // Ignore functions in the query
    if (left instanceof FunctionResult || right instanceof FunctionResult)
      return null;
    decodeResults(left, right, fieldName, value);
    // We need to check to see if we are in a NOT context. If so,
    // then we need to reverse the negation.
    boolean negated = false;
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      if (ctx.inNotContext)
        negated = !negated;
    }
    QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
    this.currentNode.getTerms().put(fieldName.toString(), term);
    return null;
  }
  
  public Object visit(ASTGENode node, Object data) {
    StringBuilder fieldName = new StringBuilder();
    ObjectHolder value = new ObjectHolder();
    // Process both sides of this node.
    Object left = node.jjtGetChild(0).jjtAccept(this, data);
    Object right = node.jjtGetChild(1).jjtAccept(this, data);
    // Ignore functions in the query
    if (left instanceof FunctionResult || right instanceof FunctionResult)
      return null;
    decodeResults(left, right, fieldName, value);
    // We need to check to see if we are in a NOT context. If so,
    // then we need to reverse the negation.
    boolean negated = false;
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      if (ctx.inNotContext)
        negated = !negated;
    }
    QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
    this.currentNode.getTerms().put(fieldName.toString(), term);
    return null;
  }
  
  public Object visit(ASTERNode node, Object data) {
    StringBuilder fieldName = new StringBuilder();
    ObjectHolder value = new ObjectHolder();
    // Process both sides of this node.
    Object left = node.jjtGetChild(0).jjtAccept(this, data);
    Object right = node.jjtGetChild(1).jjtAccept(this, data);
    // Ignore functions in the query
    if (left instanceof FunctionResult || right instanceof FunctionResult)
      return null;
    decodeResults(left, right, fieldName, value);
    // We need to check to see if we are in a NOT context. If so,
    // then we need to reverse the negation.
    boolean negated = false;
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      if (ctx.inNotContext)
        negated = !negated;
    }
    QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
    this.currentNode.getTerms().put(fieldName.toString(), term);
    return null;
  }
  
  public Object visit(ASTNRNode node, Object data) {
    StringBuilder fieldName = new StringBuilder();
    ObjectHolder value = new ObjectHolder();
    // Process both sides of this node.
    Object left = node.jjtGetChild(0).jjtAccept(this, data);
    Object right = node.jjtGetChild(1).jjtAccept(this, data);
    // Ignore functions in the query
    if (left instanceof FunctionResult || right instanceof FunctionResult)
      return null;
    decodeResults(left, right, fieldName, value);
    // We need to check to see if we are in a NOT context. If so,
    // then we need to reverse the negation.
    boolean negated = true;
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      if (ctx.inNotContext)
        negated = !negated;
    }
    QueryTerm term = new QueryTerm(negated, "!~", value.getObject());
    this.currentNode.getTerms().put(fieldName.toString(), term);
    return null;
  }
  
  public Object visit(ASTAdditiveNode node, Object data) {
    return null;
  }
  
  public Object visit(ASTAdditiveOperator node, Object data) {
    return null;
  }
  
  public Object visit(ASTMulNode node, Object data) {
    return null;
  }
  
  public Object visit(ASTDivNode node, Object data) {
    return null;
  }
  
  public Object visit(ASTModNode node, Object data) {
    return null;
  }
  
  public Object visit(ASTUnaryMinusNode node, Object data) {
    return null;
  }
  
  public Object visit(ASTBitwiseComplNode node, Object data) {
    return null;
  }
  
  public Object visit(ASTNotNode node, Object data) {
    boolean previouslyInNotContext = false;
    EvaluationContext ctx = null;
    if (null != data && data instanceof EvaluationContext) {
      ctx = (EvaluationContext) data;
      previouslyInNotContext = ctx.inNotContext;
    } else {
      ctx = new EvaluationContext();
    }
    ctx.inNotContext = true;
    // Create a new node in the tree to represent the NOT
    // Create a new And node under the current node.
    TreeNode notNode = new TreeNode();
    notNode.setType(ASTNotNode.class);
    notNode.setParent(this.currentNode);
    this.currentNode.getChildren().add(notNode);
    this.currentNode = notNode;
    // Process both sides of this node.
    node.jjtGetChild(0).jjtAccept(this, ctx);
    // reset the state
    if (null != data && !previouslyInNotContext)
      ctx.inNotContext = false;
    return null;
  }
  
  public Object visit(ASTIdentifier node, Object data) {
    return new TermResult(node.image);
  }
  
  public Object visit(ASTNullLiteral node, Object data) {
    return new LiteralResult(node.image);
  }
  
  public Object visit(ASTTrueNode node, Object data) {
    return new LiteralResult(node.image);
  }
  
  public Object visit(ASTFalseNode node, Object data) {
    return new LiteralResult(node.image);
  }
  
  public Object visit(ASTIntegerLiteral node, Object data) {
    return new LiteralResult(node.image);
  }
  
  public Object visit(ASTFloatLiteral node, Object data) {
    return new LiteralResult(node.image);
  }
  
  public Object visit(ASTStringLiteral node, Object data) {
    return new LiteralResult("'" + node.image + "'");
  }
  
  public Object visit(ASTArrayLiteral node, Object data) {
    return null;
  }
  
  public Object visit(ASTMapLiteral node, Object data) {
    return null;
  }
  
  public Object visit(ASTMapEntry node, Object data) {
    return null;
  }
  
  public Object visit(ASTEmptyFunction node, Object data) {
    return null;
  }
  
  public Object visit(ASTSizeFunction node, Object data) {
    return null;
  }
  
  public Object visit(ASTFunctionNode node, Object data) {
    // objectNode 0 is the prefix
    // objectNode 1 is the identifier , the others are parameters.
    // process the remaining arguments
    FunctionResult fr = new FunctionResult();
    int argc = node.jjtGetNumChildren() - 2;
    for (int i = 0; i < argc; i++) {
      // Process both sides of this node.
      Object result = node.jjtGetChild(i + 2).jjtAccept(this, data);
      if (result instanceof TermResult) {
        TermResult tr = (TermResult) result;
        fr.getTerms().add(tr);
      }
    }
    return fr;
  }
  
  public Object visit(ASTMethodNode node, Object data) {
    return null;
  }
  
  public Object visit(ASTSizeMethod node, Object data) {
    return null;
  }
  
  public Object visit(ASTConstructorNode node, Object data) {
    return null;
  }
  
  public Object visit(ASTArrayAccess node, Object data) {
    return null;
  }
  
  public Object visit(ASTReference node, Object data) {
    return node.jjtGetChild(0).jjtAccept(this, data);
  }
  
  private void decodeResults(Object left, Object right, StringBuilder fieldName, ObjectHolder holder) {
    if (left instanceof TermResult) {
      TermResult tr = (TermResult) left;
      fieldName.append((String) tr.value);
      // Then the right has to be the value
      if (right instanceof LiteralResult) {
        holder.setObject(((LiteralResult) right).value);
      } else {
        throw new IllegalArgumentException("Object mismatch");
      }
    } else if (right instanceof TermResult) {
      TermResult tr = (TermResult) right;
      fieldName.append((String) tr.value);
      if (left instanceof LiteralResult) {
        holder.setObject(((LiteralResult) left).value);
      } else {
        throw new IllegalArgumentException("Object mismatch");
      }
      
    } else {
      throw new IllegalArgumentException("No Term specified in query");
    }
  }
}
