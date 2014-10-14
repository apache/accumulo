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
package org.apache.accumulo.test.replication.merkle;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.accumulo.core.util.Pair;

import com.google.common.collect.Iterables;

/**
 * Simple implementation of a Merkle tree
 */
public class MerkleTree {
  protected List<MerkleTreeNode> leaves;
  protected String digestAlgorithm;

  public MerkleTree(List<MerkleTreeNode> leaves, String digestAlgorithm) {
    this.leaves = leaves;
    this.digestAlgorithm = digestAlgorithm;
  }

  public MerkleTreeNode getRootNode() throws NoSuchAlgorithmException {
    ArrayList<MerkleTreeNode> buffer = new ArrayList<>(leaves.size());
    buffer.addAll(leaves);

    while (buffer.size() > 1) {
      // Find two nodes that we want to roll up
      Pair<Integer,Integer> pairToJoin = findNextPair(buffer);

      // Make a parent node from them
      MerkleTreeNode parent = new MerkleTreeNode(Arrays.asList(buffer.get(pairToJoin.getFirst()), buffer.get(pairToJoin.getSecond())), digestAlgorithm);

      // Insert it back into the "tree" at the position of the first child
      buffer.set(pairToJoin.getFirst(), parent);

      // Remove the second child completely
      buffer.remove(pairToJoin.getSecond().intValue());

      // "recurse"
    }

    return Iterables.getOnlyElement(buffer);
  }

  protected Pair<Integer,Integer> findNextPair(List<MerkleTreeNode> nodes) {
    int i = 0, j = 1;
    while (i < nodes.size() && j < nodes.size()) {
      MerkleTreeNode left = nodes.get(i), right = nodes.get(j);

      // At the same level
      if (left.getLevel() == right.getLevel()) {
        return new Pair<Integer,Integer>(i, j);
      }

      // Peek to see if we have another element
      if (j + 1 < nodes.size()) {
        // If we do, try to match those
        i++;
        j++;
      } else {
        // Otherwise, the last two elements must be paired
        return new Pair<Integer,Integer>(i, j);
      }
    }

    if (2 < nodes.size()) {
      throw new IllegalStateException("Should not have exited loop without pairing two elements when we have at least 3 nodes");
    } else if (2 == nodes.size()) {
      return new Pair<Integer,Integer>(0, 1);
    } else {
      throw new IllegalStateException("Must have at least two nodes to pair");
    }
  }
}
