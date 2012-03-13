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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

/**
 * Validate the column visibility is a valid expression and set the visibility for a Mutation. See {@link ColumnVisibility#ColumnVisibility(byte[])} for the
 * definition of an expression.
 */
public class ColumnVisibility {
  
  Node node = null;
  private byte[] expression;
  
  /**
   * Accessor for the underlying byte string.
   * 
   * @return byte array representation of a visibility expression
   */
  public byte[] getExpression() {
    return expression;
  }
  
  public static enum NodeType {
    TERM, OR, AND,
  }
  
  public static class Node {
    public final static List<Node> EMPTY = Collections.emptyList();
    NodeType type;
    int start = 0;
    int end = 0;
    List<Node> children = EMPTY;
    
    public Node(NodeType type) {
      this.type = type;
    }
    
    public Node(int start, int end) {
      this.type = NodeType.TERM;
      this.start = start;
      this.end = end;
    }
    
    public void add(Node child) {
      if (children == EMPTY)
        children = new ArrayList<Node>();
      
      children.add(child);
    }
    
    public NodeType getType() {
      return type;
    }
    
    public List<Node> getChildren() {
      return children;
    }
    
    public int getTermStart() {
      return start;
    }
    
    public int getTermEnd() {
      return end;
    }
  }
  
  public static class NodeComparator implements Comparator<Node> {
    
    byte[] text;
    
    NodeComparator(byte[] text) {
      this.text = text;
    }
    
    @Override
    public int compare(Node a, Node b) {
      int diff = a.type.ordinal() - b.type.ordinal();
      if (diff != 0)
        return diff;
      switch (a.type) {
        case TERM:
          return WritableComparator.compareBytes(text, a.start, a.end - a.start, text, b.start, b.end - b.start);
        case OR:
        case AND:
          diff = a.children.size() - b.children.size();
          if (diff != 0)
            return diff;
          for (int i = 0; i < a.children.size(); i++) {
            diff = compare(a.children.get(i), b.children.get(i));
            if (diff != 0)
              return diff;
          }
      }
      return 0;
    }
  }
  
  static private void flatten(Node root, byte[] expression, StringBuilder out) {
    if (root.type == NodeType.TERM)
      out.append(new String(expression, root.start, root.end - root.start));
    else {
      String sep = "";
      Collections.sort(root.children, new NodeComparator(expression));
      for (Node c : root.children) {
        out.append(sep);
        boolean parens = (c.type != NodeType.TERM && root.type != c.type);
        if (parens)
          out.append("(");
        flatten(c, expression, out);
        if (parens)
          out.append(")");
        sep = root.type == NodeType.AND ? "&" : "|";
      }
    }
  }
  
  public byte[] flatten() {
    StringBuilder builder = new StringBuilder();
    flatten(node, expression, builder);
    return builder.toString().getBytes();
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
        return new Node(start, end);
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
              result = new Node(NodeType.AND);
            }
            result.add(expr);
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
              result = new Node(NodeType.OR);
            }
            result.add(expr);
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
              for (Node c : child.children)
                result.add(c);
            else
              result.add(child);
            result.end = index - 1;
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
        result.add(child);
      else
        result = child;
      if (result.type != NodeType.TERM)
        if (result.children.size() < 2)
          throw new BadArgumentException("missing term", new String(expression), index);
      return result;
    }
  }
  
  private void validate(byte[] expression) {
    if (expression != null && expression.length > 0) {
      ColumnVisibilityParser p = new ColumnVisibilityParser();
      node = p.parse(expression);
    }
    this.expression = expression;
  }
  
  /**
   * Empty visibility. Normally, elements with empty visibility can be seen by everyone. Though, one could change this behavior with filters.
   */
  public ColumnVisibility() {
    expression = new byte[0];
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
    return "[" + new String(expression) + "]";
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
  public boolean equals(ColumnVisibility otherLe) {
    return Arrays.equals(expression, otherLe.expression);
  }
  
  @Override
  public int hashCode() {
    return Arrays.hashCode(expression);
  }
  
  public Node getParseTree() {
    return node;
  }
}
