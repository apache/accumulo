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
package org.apache.accumulo.core.client.lexicoder;

import static org.apache.accumulo.core.client.lexicoder.util.ByteUtils.concat;
import static org.apache.accumulo.core.client.lexicoder.util.ByteUtils.escape;
import static org.apache.accumulo.core.client.lexicoder.util.ByteUtils.split;
import static org.apache.accumulo.core.client.lexicoder.util.ByteUtils.unescape;

import org.apache.accumulo.core.util.ComparablePair;

public class PairLexicoder<A extends Comparable<A>,B extends Comparable<B>> implements Lexicoder<ComparablePair<A,B>> {
  
  private Lexicoder<A> firstLexicoder;
  private Lexicoder<B> secondLexicoder;
  
  public PairLexicoder(Lexicoder<A> firstLexicoder, Lexicoder<B> secondLexicoder) {
    this.firstLexicoder = firstLexicoder;
    this.secondLexicoder = secondLexicoder;
  }
  
  @Override
  public byte[] encode(ComparablePair<A,B> data) {
    return concat(escape(firstLexicoder.encode(data.getFirst())), escape(secondLexicoder.encode(data.getSecond())));
  }
  
  @Override
  public ComparablePair<A,B> decode(byte[] data) {
    
    byte[][] fields = split(data);
    if (fields.length != 2) {
      throw new RuntimeException("Data does not have 2 fields, it has " + fields.length);
    }
    
    return new ComparablePair<A,B>(firstLexicoder.decode(unescape(fields[0])), secondLexicoder.decode(unescape(fields[1])));
  }
  
}
