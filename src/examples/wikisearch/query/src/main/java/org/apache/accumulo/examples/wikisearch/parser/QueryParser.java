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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.map.LRUMap;
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
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.Parser;
import org.apache.commons.jexl2.parser.ParserVisitor;
import org.apache.commons.jexl2.parser.SimpleNode;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.util.hash.MurmurHash;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Parses the query for the purposes of extracting terms, operators, and literals for query optimization. This class does not necessarily understand how to
 * parse all of the possible combinations of the JEXL syntax, but that does not mean that the query will not evaluate against the event objects. It means that
 * the unsupported operators will not be parsed and included in the optimization step.
 * 
 */
public class QueryParser implements ParserVisitor {
  
  public static class QueryTerm {
    private boolean negated = false;
    private String operator = null;
    private Object value = null;
    
    public QueryTerm(boolean negated, String operator, Object value) {
      super();
      this.negated = negated;
      this.operator = operator;
      this.value = value;
    }
    
    public boolean isNegated() {
      return negated;
    }
    
    public String getOperator() {
      return operator;
    }
    
    public Object getValue() {
      return value;
    }
    
    public void setNegated(boolean negated) {
      this.negated = negated;
    }
    
    public void setOperator(String operator) {
      this.operator = operator;
    }
    
    public void setValue(Object value) {
      this.value = value;
    }
    
    public String toString() {
      StringBuilder buf = new StringBuilder();
      buf.append("negated: ").append(negated).append(", operator: ").append(operator).append(", value: ").append(value);
      return buf.toString();
    }
  }
  
  /**
   * Holder object
   */
  static class ObjectHolder {
    Object object;
    
    public Object getObject() {
      return object;
    }
    
    public void setObject(Object object) {
      this.object = object;
    }
  }
  
  static class FunctionResult {
    private List<TermResult> terms = new ArrayList<TermResult>();
    
    public List<TermResult> getTerms() {
      return terms;
    }
  }
  
  /**
   * Holder object for a term (i.e. field name)
   */
  static class TermResult {
    Object value;
    
    public TermResult(Object value) {
      this.value = value;
    }
  }
  
  /**
   * Holder object for a literal (integer, float, string, or null literal) value
   */
  static class LiteralResult {
    Object value;
    
    public LiteralResult(Object value) {
      this.value = value;
    }
  }
  
  /**
   * Object used to store context information as the AST is being iterated over.
   */
  static class EvaluationContext {
    boolean inOrContext = false;
    boolean inNotContext = false;
    boolean inAndContext = false;
  }
  
  /**
   * Object to store information from previously parsed queries.
   */
  private static class CacheEntry {
    private Set<String> negatedTerms = null;
    private Set<String> andTerms = null;
    private Set<String> orTerms = null;
    private Set<Object> literals = null;
    private Multimap<String,QueryTerm> terms = null;
    private ASTJexlScript rootNode = null;
    private TreeNode tree = null;
    
    public CacheEntry(Set<String> negatedTerms, Set<String> andTerms, Set<String> orTerms, Set<Object> literals, Multimap<String,QueryTerm> terms,
        ASTJexlScript rootNode, TreeNode tree) {
      super();
      this.negatedTerms = negatedTerms;
      this.andTerms = andTerms;
      this.orTerms = orTerms;
      this.literals = literals;
      this.terms = terms;
      this.rootNode = rootNode;
      this.tree = tree;
    }
    
    public Set<String> getNegatedTerms() {
      return negatedTerms;
    }
    
    public Set<String> getAndTerms() {
      return andTerms;
    }
    
    public Set<String> getOrTerms() {
      return orTerms;
    }
    
    public Set<Object> getLiterals() {
      return literals;
    }
    
    public Multimap<String,QueryTerm> getTerms() {
      return terms;
    }
    
    public ASTJexlScript getRootNode() {
      return rootNode;
    }
    
    public TreeNode getTree() {
      return tree;
    }
  }
  
  private static final int SEED = 650567;
  
  private static LRUMap cache = new LRUMap();
  
  protected Set<String> negatedTerms = new HashSet<String>();
  
  private Set<String> andTerms = new HashSet<String>();
  
  private Set<String> orTerms = new HashSet<String>();
  
  /**
   * List of String, Integer, Float, etc literals that were passed in the query
   */
  private Set<Object> literals = new HashSet<Object>();
  
  /**
   * Map of terms (field names) to QueryTerm objects.
   */
  private Multimap<String,QueryTerm> terms = HashMultimap.create();
  
  private ASTJexlScript rootNode = null;
  
  private TreeNode tree = null;
  
  private int hashVal = 0;
  
  public QueryParser() {}
  
  private void reset() {
    this.negatedTerms.clear();
    this.andTerms.clear();
    this.orTerms.clear();
    this.literals.clear();
    this.terms = HashMultimap.create();
  }
  
  public void execute(String query) throws ParseException {
    reset();
    query = query.replaceAll("\\s+AND\\s+", " and ");
    query = query.replaceAll("\\s+OR\\s+", " or ");
    query = query.replaceAll("\\s+NOT\\s+", " not ");
    
    // Check to see if its in the cache
    Hash hash = MurmurHash.getInstance();
    this.hashVal = hash.hash(query.getBytes(), SEED);
    CacheEntry entry = null;
    synchronized (cache) {
      entry = (CacheEntry) cache.get(hashVal);
    }
    if (entry != null) {
      this.negatedTerms = entry.getNegatedTerms();
      this.andTerms = entry.getAndTerms();
      this.orTerms = entry.getOrTerms();
      this.literals = entry.getLiterals();
      this.terms = entry.getTerms();
      this.rootNode = entry.getRootNode();
      this.tree = entry.getTree();
    } else {
      Parser p = new Parser(new StringReader(";"));
      rootNode = p.parse(new StringReader(query), null);
      rootNode.childrenAccept(this, null);
      TreeBuilder builder = new TreeBuilder(rootNode);
      tree = builder.getRootNode();
      entry = new CacheEntry(this.negatedTerms, this.andTerms, this.orTerms, this.literals, this.terms, rootNode, tree);
      synchronized (cache) {
        cache.put(hashVal, entry);
      }
    }
    
  }
  
  /**
   * 
   * @return this queries hash value
   */
  public int getHashValue() {
    return this.hashVal;
  }
  
  public TreeNode getIteratorTree() {
    return this.tree;
  }
  
  /**
   * 
   * @return JEXL abstract syntax tree
   */
  public ASTJexlScript getAST() {
    return this.rootNode;
  }
  
  /**
   * 
   * @return Set of field names to use in the optimizer for nots. As a general rule none of these terms should be used to find an event and should they should
   *         be evaluated on each event after being found.
   */
  public Set<String> getNegatedTermsForOptimizer() {
    return negatedTerms;
  }
  
  /**
   * 
   * @return Set of field names to use in the optimizer for ands. As a general rule any one term of an and clause can be used to find associated events.
   */
  public Set<String> getAndTermsForOptimizer() {
    return andTerms;
  }
  
  /**
   * 
   * @return Set of field names to use in the optimizer for ors. As a general rule any terms that are part of an or clause need to be searched to find the
   *         associated events.
   */
  public Set<String> getOrTermsForOptimizer() {
    return orTerms;
  }
  
  /**
   * 
   * @return String, Integer, and Float literals used in the query.
   */
  public Set<Object> getQueryLiterals() {
    return literals;
  }
  
  /**
   * 
   * @return Set of all identifiers (field names) in the query.
   */
  public Set<String> getQueryIdentifiers() {
    return terms.keySet();
  }
  
  /**
   * 
   * @return map of term (field name) to QueryTerm object
   */
  public Multimap<String,QueryTerm> getQueryTerms() {
    return terms;
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
    // Process both sides of this node.
    node.jjtGetChild(0).jjtAccept(this, ctx);
    node.jjtGetChild(1).jjtAccept(this, ctx);
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
    // Process both sides of this node.
    node.jjtGetChild(0).jjtAccept(this, ctx);
    node.jjtGetChild(1).jjtAccept(this, ctx);
    // reset the state
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
    terms.put(fieldName.toString(), term);
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
    if (negated)
      negatedTerms.add(fieldName.toString());
    QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
    terms.put(fieldName.toString(), term);
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
    terms.put(fieldName.toString(), term);
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
    terms.put(fieldName.toString(), term);
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
    terms.put(fieldName.toString(), term);
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
    terms.put(fieldName.toString(), term);
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
    terms.put(fieldName.toString(), term);
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
    if (negated)
      negatedTerms.add(fieldName.toString());
    QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
    terms.put(fieldName.toString(), term);
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
    // Process both sides of this node.
    node.jjtGetChild(0).jjtAccept(this, ctx);
    // reset the state
    if (null != data && !previouslyInNotContext)
      ctx.inNotContext = false;
    return null;
  }
  
  public Object visit(ASTIdentifier node, Object data) {
    if (data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      if (ctx.inAndContext)
        andTerms.add(node.image);
      if (ctx.inNotContext)
        negatedTerms.add(node.image);
      if (ctx.inOrContext)
        orTerms.add(node.image);
    }
    return new TermResult(node.image);
  }
  
  public Object visit(ASTNullLiteral node, Object data) {
    literals.add(node.image);
    return new LiteralResult(node.image);
  }
  
  public Object visit(ASTTrueNode node, Object data) {
    return new LiteralResult(node.image);
  }
  
  public Object visit(ASTFalseNode node, Object data) {
    return new LiteralResult(node.image);
  }
  
  public Object visit(ASTIntegerLiteral node, Object data) {
    literals.add(node.image);
    return new LiteralResult(node.image);
  }
  
  public Object visit(ASTFloatLiteral node, Object data) {
    literals.add(node.image);
    return new LiteralResult(node.image);
  }
  
  public Object visit(ASTStringLiteral node, Object data) {
    literals.add("'" + node.image + "'");
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
    // We need to check to see if we are in a NOT context. If so,
    // then we need to reverse the negation.
    boolean negated = true;
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      if (ctx.inNotContext)
        negated = !negated;
    }
    // used to rebuild function call from the AST
    StringBuilder buf = new StringBuilder();
    String sep = "";
    // objectNode 0 is the prefix
    buf.append(node.jjtGetChild(0).image).append(":");
    // objectNode 1 is the identifier , the others are parameters.
    buf.append(node.jjtGetChild(1).image).append("(");
    // process the remaining arguments
    FunctionResult fr = new FunctionResult();
    int argc = node.jjtGetNumChildren() - 2;
    for (int i = 0; i < argc; i++) {
      // Process both sides of this node.
      Object result = node.jjtGetChild(i + 2).jjtAccept(this, data);
      if (result instanceof TermResult) {
        TermResult tr = (TermResult) result;
        fr.getTerms().add(tr);
        buf.append(sep).append(tr.value);
        sep = ", ";
      } else {
        buf.append(sep).append(node.jjtGetChild(i + 2).image);
        sep = ", ";
      }
    }
    buf.append(")");
    // Capture the entire function call for each function parameter
    for (TermResult tr : fr.terms)
      terms.put((String) tr.value, new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), buf.toString()));
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
  
  protected void decodeResults(Object left, Object right, StringBuilder fieldName, ObjectHolder holder) {
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
