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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the level (height) within the tree, the ranges that it covers, and the new hash
 */
public class MerkleTreeNode {
  private static final Logger log = LoggerFactory.getLogger(MerkleTreeNode.class);

  private Range range;
  private int level;
  private List<Range> children;
  private byte[] hash;

  public MerkleTreeNode(Range range, int level, List<Range> children, byte[] hash) {
    this.range = range;
    this.level = level;
    this.children = children;
    this.hash = hash;
  }

  public MerkleTreeNode(Key k, Value v) {
    range = RangeSerialization.toRange(k);
    level = 0;
    children = Collections.emptyList();
    hash = v.get();
  }

  public MerkleTreeNode(List<MerkleTreeNode> children, String digestAlgorithm) throws NoSuchAlgorithmException {
    level = 0;
    this.children = new ArrayList<Range>(children.size());
    MessageDigest digest = MessageDigest.getInstance(digestAlgorithm);

    Range childrenRange = null;
    for (MerkleTreeNode child : children) {
      this.children.add(child.getRange());
      level = Math.max(child.getLevel(), level);
      digest.update(child.getHash());

      if (null == childrenRange) {
        childrenRange = child.getRange();
      } else {
        List<Range> overlappingRanges = Range.mergeOverlapping(Arrays.asList(childrenRange, child.getRange()));
        if (1 != overlappingRanges.size()) {
          log.error("Tried to merge non-contiguous ranges: {} {}", childrenRange, child.getRange());
          throw new IllegalArgumentException("Ranges must be contiguous: " + childrenRange + ", " + child.getRange());
        }

        childrenRange = overlappingRanges.get(0);
      }
    }

    // Our actual level is one more than the highest level of our children
    level++;

    // Roll the hash up the tree
    hash = digest.digest();

    // Set the range to be the merged result of the children
    range = childrenRange;
  }

  public Range getRange() {
    return range;
  }

  public int getLevel() {
    return level;
  }

  public List<Range> getChildren() {
    return children;
  }

  public byte[] getHash() {
    return hash;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(32);
    sb.append("range=").append(range).append(" level=").append(level).append(" hash=").append(Hex.encodeHexString(hash)).append(" children=").append(children);
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof MerkleTreeNode) {
      MerkleTreeNode other = (MerkleTreeNode) o;
      return range.equals(other.getRange()) && level == other.getLevel() && children.equals(other.getChildren()) && Arrays.equals(hash, other.getHash());
    }

    return false;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder(1395, 39532);
    return hcb.append(range).append(level).append(children).append(hash).toHashCode();
  }
}
