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
package org.apache.accumulo.core.security;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;

/**
 * Validate the column visibility is a valid expression and set the visibility for a Mutation. See {@link ColumnVisibility#ColumnVisibility(byte[])} for the
 * definition of an expression.
 */
public class ColumnVisibility {
  
  private Node node = null;
  
  public static enum NodeType {
    TERM, OR, AND,
  }

  private static abstract class Node implements Comparable<Node> {
    protected final NodeType type;
    
    public Node(NodeType type)
    {
      this.type = type;
    }

    public byte[] generate() {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      generate(baos,false);
      return baos.toByteArray();
    }
    
    public abstract boolean evaluate(Authorizations auths);
    
    protected abstract void generate(ByteArrayOutputStream baos, boolean parens);
  }
  
  private static class TermNode extends Node {
    
    final ByteSequence bs;
    
    public TermNode(final ByteSequence bs) {
      super(NodeType.TERM);
      this.bs = bs;
    }
    
    public boolean evaluate(Authorizations auths)
    {
      return auths.contains(bs);
    }


    protected void generate(ByteArrayOutputStream baos, boolean parens)
    {
      baos.write(bs.getBackingArray(), bs.offset(), bs.length());
    }
    
    @Override
    public boolean equals(Object other) {
      if(other instanceof TermNode)
      {
        return bs.compareTo(((TermNode)other).bs) == 0;
      }
      return false;
    }
    
    @Override
    public int compareTo(Node o) {
      if(o.type == NodeType.TERM)
      {
        return bs.compareTo(((TermNode)o).bs);
      }
      return type.ordinal() - o.type.ordinal();
    }
  }
  
  private abstract static class AggregateNode extends Node {

    /**
     * @param type
     */
    public AggregateNode(NodeType type) {
      super(type);
    }
    
    protected TreeSet<Node> children = new TreeSet<Node>();
    
    protected abstract byte getOperator();
    
    @Override
    protected void generate(ByteArrayOutputStream baos, boolean parens) {
      if(parens)
        baos.write('(');
      boolean first = true;
      for(Node child:children)
      {
        if(!first)
          baos.write(getOperator());
        child.generate(baos, true);
        first = false;
      }
      if(parens)
        baos.write(')');
    }
    
    @Override
    public int compareTo(Node o) {
      int ordinalDiff = type.ordinal() - o.type.ordinal();
      if(ordinalDiff != 0)
        return ordinalDiff;
      AggregateNode other = (AggregateNode)o;
      int childCountDifference = children.size() - other.children.size();
      if(childCountDifference != 0)
        return childCountDifference;
      Iterator<Node> otherChildren = other.children.iterator();
      for(Node n1:children)
      {
        int comp = n1.compareTo(otherChildren.next());
        if(comp != 0)
          return comp;
      }
      return 0;
    }

  }
  
  private static class OrNode extends AggregateNode {

    public OrNode() {
      super(NodeType.OR);
    }

    @Override
    public boolean evaluate(Authorizations auths) {
      for(Node child:children)
        if(child.evaluate(auths))
          return true;
      return false;
    }

    @Override
    protected byte getOperator() {
      return '|';
    }
    
  }
  
  private static class AndNode extends AggregateNode {

    public AndNode()
    {
      super(NodeType.AND);
    }
    
    @Override
    public boolean evaluate(Authorizations auths) {
      for(Node child:children)
        if(!child.evaluate(auths))
          return false;
      return true;
    }

    @Override
    protected byte getOperator() {
      return '&';
    }
    
  }

  private byte[] expression = null;
  
  /**
   * @deprecated
   * @see org.apache.accumulo.security.ColumnVisibility#getExpression()
   */
  public byte[] flatten() {
    return getExpression();
  }
  
  /**
   * Generate the byte[] that represents this ColumnVisibility.
   * @return a byte[] representation of this visibility
   */
  public byte[] getExpression(){
    if(expression != null)
      return expression;
    expression = _flatten();
    return expression;
  }
  
  private static final byte[] emptyExpression = new byte[0];
  
  private byte[] _flatten() {
    if(node == null)
      return emptyExpression;
    return node.generate();
  }
  
  private static class ColumnVisibilityParser {
    private int index = 0;
    private int parens = 0;
    
    public ColumnVisibilityParser() {}
    
    Node parse(byte[] expression) {
      if (expression.length > 0) {
        Node node = parse_(expression);
        if (node == null) {
          throw new BadArgumentException("operator or missing parens", new String(expression), index - 1);
        }
        if (parens != 0) {
          throw new BadArgumentException("parenthesis mis-match", new String(expression), index - 1);
        }
        return node;
      }
      return null;
    }
    
    Node processTerm(int start, int end, Node expr, byte[] expression) {
      if (start != end) {
        if (expr != null)
          throw new BadArgumentException("expression needs | or &", new String(expression), start);
        return new TermNode(new ArrayByteSequence(expression, start, end - start));
      }
      if (expr == null)
        throw new BadArgumentException("empty term", new String(expression), start);
      return expr;
    }
    
    Node parse_(byte[] expression) {
      Node result = null;
      Node expr = null;
      int termStart = index;
      while (index < expression.length) {
        switch (expression[index++]) {
          case '&': {
            expr = processTerm(termStart, index - 1, expr, expression);
            if (result != null) {
              if (!result.type.equals(NodeType.AND))
                throw new BadArgumentException("cannot mix & and |", new String(expression), index - 1);
            } else {
              result = new AndNode();
            }
            ((AggregateNode)result).children.add(expr);
            expr = null;
            termStart = index;
            break;
          }
          case '|': {
            expr = processTerm(termStart, index - 1, expr, expression);
            if (result != null) {
              if (!result.type.equals(NodeType.OR))
                throw new BadArgumentException("cannot mix | and &", new String(expression), index - 1);
            } else {
              result = new OrNode();
            }
            ((AggregateNode)result).children.add(expr);
            expr = null;
            termStart = index;
            break;
          }
          case '(': {
            parens++;
            if (termStart != index - 1 || expr != null)
              throw new BadArgumentException("expression needs & or |", new String(expression), index - 1);
            expr = parse_(expression);
            termStart = index;
            break;
          }
          case ')': {
            parens--;
            Node child = processTerm(termStart, index - 1, expr, expression);
            if (child == null && result == null)
              throw new BadArgumentException("empty expression not allowed", new String(expression), index);
            if (result == null)
              return child;
            if (result.type == child.type)
            {
              AggregateNode parenNode = (AggregateNode)child;
              for (Node c : parenNode.children)
                ((AggregateNode)result).children.add(c);
            }
            else
              ((AggregateNode)result).children.add(child);
            if (result.type != NodeType.TERM)
            {
              AggregateNode resultNode = (AggregateNode)result;
              if (resultNode.children.size() == 1)
                return resultNode.children.first();
              if (resultNode.children.size() < 2)
                throw new BadArgumentException("missing term", new String(expression), index);
            }
            return result;
          }
          default: {
            byte c = expression[index - 1];
            if (!Authorizations.isValidAuthChar(c))
              throw new BadArgumentException("bad character (" + c + ")", new String(expression), index - 1);
          }
        }
      }
      Node child = processTerm(termStart, index, expr, expression);
      if (result != null)
      {
        if(result.type == child.type)
        {
          ((AggregateNode)result).children.addAll(((AggregateNode)child).children);
        }
        else
          ((AggregateNode)result).children.add(child);
      }
      else
        result = child;
      if (result.type != NodeType.TERM)
      {
        AggregateNode resultNode = (AggregateNode)result;
        if (resultNode.children.size() == 1)
          return resultNode.children.first();
        if (resultNode.children.size() < 2)
          throw new BadArgumentException("missing term", new String(expression), index);
      }
      return result;
    }
  }
  
  private void validate(byte[] expression) {
    if (expression != null && expression.length > 0) {
      ColumnVisibilityParser p = new ColumnVisibilityParser();
      node = p.parse(expression);
    }
  }
  
  /**
   * Empty visibility. Normally, elements with empty visibility can be seen by everyone. Though, one could change this behavior with filters.
   */
  public ColumnVisibility() {
  }
  
  /**
   * See {@link #ColumnVisibility(byte[])}
   * 
   * @param expression
   */
  public ColumnVisibility(String expression) {
    this(expression.getBytes());
  }
  
  public ColumnVisibility(Text expression) {
    this(TextUtil.getBytes(expression));
  }
  
  private ColumnVisibility(Node node) {
    this.node = node;
  }
  
  /**
   * Set the column visibility for a Mutation.
   * 
   * @param expression
   *          An expression of the rights needed to see this mutation. The expression is a sequence of characters from the set [A-Za-z0-9_-] along with the
   *          binary operators "&" and "|" indicating that both operands are necessary, or the either is necessary. The following are valid expressions for
   *          visibility:
   * 
   *          <pre>
   * A
   * A|B
   * (A|B)&(C|D)
   * orange|(red&yellow)
   * 
   * </pre>
   * 
   *          The following are not valid expressions for visibility:
   * 
   *          <pre>
   * A|B&C
   * A=B
   * A|B|
   * A&|B
   * ()
   * )
   * dog|!cat
   * </pre>
   */
  public ColumnVisibility(byte[] expression) {
    validate(expression);
  }
  
  @Override
  public String toString() {
    return "[" + new String(this.getExpression()) + "]";
  }
  
  /**
   * See {@link #equals(ColumnVisibility)}
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ColumnVisibility)
      return equals((ColumnVisibility) obj);
    return false;
  }
  
  /**
   * Compares two ColumnVisibilities for string equivalence, not as a meaningful comparison of terms and conditions.
   */
//  public boolean equals(ColumnVisibility otherLe) {
//    return Arrays.equals(expression, otherLe.expression);
//  }
  
  @Override
  public int hashCode() {
    return Arrays.hashCode(getExpression());
  }
  
  public boolean evaluate(Authorizations auths) {
    if(node == null)
      return true;
    return node.evaluate(auths);
  }
  
  public ColumnVisibility or(ColumnVisibility other)
  {
    if(node == null)
      return this;
    if(other.node == null)
      return other;
    OrNode orNode = new OrNode();
    if(other.node instanceof OrNode)
      orNode.children.addAll(((OrNode)other.node).children);
    else
      orNode.children.add(other.node);
    if(node instanceof OrNode)
      orNode.children.addAll(((OrNode)node).children);
    else
      orNode.children.add(node);
    return new ColumnVisibility(orNode);
  }
  
  public ColumnVisibility and(ColumnVisibility other)
  {
    if(node == null)
      return other;
    if(other.node == null)
      return this;
    AndNode andNode = new AndNode();
    if(other.node instanceof AndNode)
      andNode.children.addAll(((AndNode)other.node).children);
    else
      andNode.children.add(other.node);
    if(node instanceof AndNode)
      andNode.children.addAll(((AndNode)node).children);
    else
      andNode.children.add(node);
    return new ColumnVisibility(andNode);
  }

}
