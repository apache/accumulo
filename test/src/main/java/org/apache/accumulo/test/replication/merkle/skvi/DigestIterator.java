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
package org.apache.accumulo.test.replication.merkle.skvi;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * {@link SortedKeyValueIterator} which attempts to compute a hash over some range of Key-Value pairs.
 * <P>
 * For the purposes of constructing a Merkle tree, this class will only generate a meaningful result if the (Batch)Scanner will compute a single digest over a
 * Range. If the (Batch)Scanner stops and restarts in the middle of a session, incorrect values will be returned and the merkle tree will be invalid.
 */
public class DigestIterator implements SortedKeyValueIterator<Key,Value> {
  public static final String HASH_NAME_KEY = "hash.name";

  private MessageDigest digest;
  private Key topKey;
  private Value topValue;
  private SortedKeyValueIterator<Key,Value> source;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    String hashName = options.get(HASH_NAME_KEY);
    if (null == hashName) {
      throw new IOException(HASH_NAME_KEY + " must be provided as option");
    }

    try {
      this.digest = MessageDigest.getInstance(hashName);
    } catch (NoSuchAlgorithmException e) {
      throw new IOException(e);
    }

    this.topKey = null;
    this.topValue = null;
    this.source = source;
  }

  @Override
  public boolean hasTop() {
    return null != topKey;
  }

  @Override
  public void next() throws IOException {
    // We can't call next() if we already consumed it all
    if (!this.source.hasTop()) {
      this.topKey = null;
      this.topValue = null;
      return;
    }

    this.source.next();

    consume();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    this.source.seek(range, columnFamilies, inclusive);

    consume();
  }

  protected void consume() throws IOException {
    digest.reset();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    if (!this.source.hasTop()) {
      this.topKey = null;
      this.topValue = null;

      return;
    }

    Key lastKeySeen = null;
    while (this.source.hasTop()) {
      baos.reset();

      Key currentKey = this.source.getTopKey();
      lastKeySeen = currentKey;

      currentKey.write(dos);
      this.source.getTopValue().write(dos);

      digest.update(baos.toByteArray());

      this.source.next();
    }

    this.topKey = lastKeySeen;
    this.topValue = new Value(digest.digest());
  }

  @Override
  public Key getTopKey() {
    return topKey;
  }

  @Override
  public Value getTopValue() {
    return topValue;
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    DigestIterator copy = new DigestIterator();
    try {
      copy.digest = MessageDigest.getInstance(digest.getAlgorithm());
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }

    copy.topKey = this.topKey;
    copy.topValue = this.topValue;
    copy.source = this.source.deepCopy(env);

    return copy;
  }

}
