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
package org.apache.accumulo.core.util;

/**
 * A class for storing data as a binary tree. This class does not implement Collection as several methods such as add are not appropriate.
 *
 * A tree is represented as a node with a parent, a left child, and a right child. If the parent is null, this node represents the root of the tree. A node has
 * contents of type T.
 *
 */

public class BinaryTree<T> {
  private BinaryTree<T> parent;
  private BinaryTree<T> left;
  private BinaryTree<T> right;

  T contents;

  public BinaryTree<T> getLeft() {
    return left;
  }

  public void setLeft(BinaryTree<T> left) {
    left.setParent(this);
    this.left = left;
  }

  public BinaryTree<T> getParent() {
    return parent;
  }

  public void setParent(BinaryTree<T> parent) {
    this.parent = parent;
  }

  public BinaryTree<T> getRight() {
    return right;
  }

  public void setRight(BinaryTree<T> right) {
    right.setParent(this);
    this.right = right;
  }

  public T getContents() {
    return contents;
  }

  public void setContents(T contents) {
    this.contents = contents;
  }

  public boolean isEmpty() {
    if (parent == null && left == null && right == null && contents == null)
      return true;
    return false;
  }

  @Override
  public String toString() {
    String out = "[";
    if (left != null)
      out += left.toString();
    out += contents;
    if (right != null)
      out += right.toString();
    out += "]";
    return out;
  }
}
