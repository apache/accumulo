/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.access;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;

import org.apache.accumulo.access.AccessExpressionImpl.Node;
import org.apache.accumulo.access.AccessExpressionImpl.NodeComparator;
import org.apache.accumulo.access.AccessExpressionImpl.NodeType;
import org.junit.jupiter.api.Test;

public class AccessExpressionImplTest {

  @Test
  public void testParseTree() {
    Node node = parse("(W)|(U&V)");
    assertNode(node, NodeType.OR, 0, 9);
    assertNode(node.getChildren().get(0), NodeType.TERM, 1, 2);
    assertNode(node.getChildren().get(1), NodeType.AND, 5, 8);
  }

  @Test
  public void testParseTreeWithNoChildren() {
    Node node = parse("ABC");
    assertNode(node, NodeType.TERM, 0, 3);
  }

  @Test
  public void testParseTreeWithTwoChildren() {
    Node node = parse("ABC|DEF");
    assertNode(node, NodeType.OR, 0, 7);
    assertNode(node.getChildren().get(0), NodeType.TERM, 0, 3);
    assertNode(node.getChildren().get(1), NodeType.TERM, 4, 7);
  }

  @Test
  public void testParseTreeWithParenthesesAndTwoChildren() {
    Node node = parse("(ABC|DEF)");
    assertNode(node, NodeType.OR, 1, 8);
    assertNode(node.getChildren().get(0), NodeType.TERM, 1, 4);
    assertNode(node.getChildren().get(1), NodeType.TERM, 5, 8);
  }

  @Test
  public void testParseTreeWithParenthesizedChildren() {
    Node node = parse("ABC|(DEF&GHI)");
    assertNode(node, NodeType.OR, 0, 13);
    assertNode(node.getChildren().get(0), NodeType.TERM, 0, 3);
    assertNode(node.getChildren().get(1), NodeType.AND, 5, 12);
    assertNode(node.getChildren().get(1).children.get(0), NodeType.TERM, 5, 8);
    assertNode(node.getChildren().get(1).children.get(1), NodeType.TERM, 9, 12);
  }

  @Test
  public void testParseTreeWithMoreParentheses() {
    Node node = parse("(W)|(U&V)");
    assertNode(node, NodeType.OR, 0, 9);
    assertNode(node.getChildren().get(0), NodeType.TERM, 1, 2);
    assertNode(node.getChildren().get(1), NodeType.AND, 5, 8);
    assertNode(node.getChildren().get(1).children.get(0), NodeType.TERM, 5, 6);
    assertNode(node.getChildren().get(1).children.get(1), NodeType.TERM, 7, 8);
  }

  @Test
  public void testEmptyParseTreesAreEqual() {
    Comparator<Node> comparator = new NodeComparator(new byte[] {});
    Node empty = new AccessExpressionImpl().getParseTree();
    assertEquals(0, comparator.compare(empty, parse("")));
  }

  @Test
  public void testParseTreesOrdering() {
    byte[] expression = "(b&c&d)|((a|m)&y&z)|(e&f)".getBytes(UTF_8);
    byte[] flattened = new AccessExpressionImpl(expression).normalize().getBytes(UTF_8);

    // Convert to String for indexOf convenience
    String flat = new String(flattened, UTF_8);
    assertTrue(flat.indexOf('e') < flat.indexOf('|'), "shortest expressions sort first");
    assertTrue(flat.indexOf('b') < flat.indexOf('a'), "shortest children sort first");
  }

  private Node parse(String s) {
    AccessExpressionImpl v = new AccessExpressionImpl(s);
    return v.getParseTree();
  }

  private void assertNode(Node node, NodeType nodeType, int start, int end) {
    assertEquals(node.type, nodeType);
    assertEquals(start, node.start);
    assertEquals(end, node.end);
  }
}
