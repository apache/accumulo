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

import static org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils.concat;
import static org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils.escape;
import static org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils.split;
import static org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils.unescape;

import org.apache.accumulo.core.clientImpl.lexicoder.AbstractLexicoder;
import org.apache.accumulo.core.util.ComparablePair;

/**
 * This class is a lexicoder that sorts a ComparablePair. Each item in the pair is encoded with the
 * given lexicoder and concatenated together. This makes it easy to construct a sortable key based
 * on two components. There are many examples of this- but a key/value relationship is a great one.
 *
 * If we decided we wanted a two-component key where the first component is a string and the second
 * component a date which is reverse sorted, we can do so with the following example:
 *
 * <pre>
 * <code>
 * StringLexicoder strEncoder = new StringLexicoder();
 * ReverseLexicoder&lt;Date&gt; dateEnc = new ReverseLexicoder&lt;&gt;(new DateLexicoder());
 * PairLexicoder&lt;String,Date&gt; pair = new PairLexicoder&lt;&gt;(strEncoder, dateEnc);
 * long now = System.currentTimeMillis();
 * byte[] pair1 = pair.encode(new ComparablePair&lt;&gt;(&quot;com&quot;, new Date(now)));
 * byte[] pair2 = pair.encode(new ComparablePair&lt;&gt;(&quot;com&quot;, new Date(now + 500)));
 * byte[] pair3 = pair.encode(new ComparablePair&lt;&gt;(&quot;org&quot;, new Date(now + 1000)));
 * </code>
 * </pre>
 *
 * In the example, pair2 will be sorted before pair1. pair3 will occur last since 'org' is sorted
 * after 'com'. If we just used a {@link DateLexicoder} instead of a {@link ReverseLexicoder}, pair1
 * would have been sorted before pair2.
 *
 * @since 1.6.0
 */
public class PairLexicoder<A extends Comparable<A>,B extends Comparable<B>>
    extends AbstractLexicoder<ComparablePair<A,B>> {

  private Lexicoder<A> firstLexicoder;
  private Lexicoder<B> secondLexicoder;

  public PairLexicoder(Lexicoder<A> firstLexicoder, Lexicoder<B> secondLexicoder) {
    this.firstLexicoder = firstLexicoder;
    this.secondLexicoder = secondLexicoder;
  }

  @Override
  public byte[] encode(ComparablePair<A,B> data) {
    return concat(escape(firstLexicoder.encode(data.getFirst())),
        escape(secondLexicoder.encode(data.getSecond())));
  }

  @Override
  public ComparablePair<A,B> decode(byte[] b) {
    // This concrete implementation is provided for binary compatibility, since the corresponding
    // superclass method has type-erased return type Object. See ACCUMULO-3789 and #1285.
    return super.decode(b);
  }

  @Override
  protected ComparablePair<A,B> decodeUnchecked(byte[] data, int offset, int len) {

    byte[][] fields = split(data, offset, len);
    if (fields.length != 2) {
      throw new RuntimeException("Data does not have 2 fields, it has " + fields.length);
    }

    return new ComparablePair<>(firstLexicoder.decode(unescape(fields[0])),
        secondLexicoder.decode(unescape(fields[1])));
  }

}
