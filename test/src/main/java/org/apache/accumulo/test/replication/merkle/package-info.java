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
/**
 * A <a href="http://en.wikipedia.org/wiki/Merkle_tree">Merkle tree</a> is a hash tree and can be used to evaluate equality over large
 * files with the ability to ascertain what portions of the files differ. Each leaf of the Merkle tree is some hash of a
 * portion of the file, with each leaf corresponding to some "range" within the source file. As such, if all leaves are
 * considered as ranges of the source file, the "sum" of all leaves creates a contiguous range over the entire file.
 * <P>
 * The parent of any nodes (typically, a binary tree; however this is not required) is the concatenation of the hashes of
 * the children. We can construct a full tree by walking up the tree, creating parents from children, until we have a root
 * node. To check equality of two files that each have a merkle tree built, we can very easily compare the value of at the
 * root of the Merkle tree to know whether or not the files are the same.
 * <P>
 * Additionally, in the situation where we have two files with we expect to be the same but are not, we can walk back down
 * the tree, finding subtrees that are equal and subtrees that are not. Subtrees that are equal correspond to portions of
 * the files which are identical, where subtrees that are not equal correspond to discrepancies between the two files.
 * <P>
 * We can apply this concept to Accumulo, treating a table as a file, and ranges within a file as an Accumulo Range. We can
 * then compute the hashes over each of these Ranges and compute the entire Merkle tree to determine if two tables are
 * equivalent.
 *
 * @since 1.7.0
 */
package org.apache.accumulo.test.replication.merkle;