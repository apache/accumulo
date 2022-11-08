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
package org.apache.accumulo.core.client.summary.summarizers;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.summary.CountingSummarizer;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.ColumnVisibility.Node;

/**
 * Counts unique authorizations in column visibility labels. Leverages super class to defend against
 * too many. This class is useful for discovering what authorizations are present when the expected
 * number of authorizations is small.
 *
 * <p>
 * As an example, assume a data set of three keys with the column visibilities :
 * {@code (A&C)|(A&D)}, {@code A&B}, and {@code C|E}. For these input this summarizer would output :
 * {@code c:A=2}, {@code c:B=1}, {@code c:C=2}, {@code D:1}, {@code E:1}. Notice that even though
 * {@code A} occurred 3 times in total, its only counted once per column visibility.
 *
 * <p>
 * See the superclass documentation for more information about usage and configuration.
 *
 * @since 2.0.0
 *
 * @see VisibilitySummarizer
 * @see TableOperations#addSummarizers(String,
 *      org.apache.accumulo.core.client.summary.SummarizerConfiguration...)
 * @see TableOperations#summaries(String)
 */
public class AuthorizationSummarizer extends CountingSummarizer<ByteSequence> {

  @Override
  protected Converter<ByteSequence> converter() {
    return new AuthsConverter();
  }

  private static class AuthsConverter implements Converter<ByteSequence> {

    final int MAX_ENTRIES = 1000;
    private Map<ByteSequence,Set<ByteSequence>> cache =
        new LinkedHashMap<>(MAX_ENTRIES + 1, .75F, true) {
          private static final long serialVersionUID = 1L;

          // This method is called just after a new entry has been added
          @Override
          public boolean removeEldestEntry(Map.Entry<ByteSequence,Set<ByteSequence>> eldest) {
            return size() > MAX_ENTRIES;
          }
        };

    @Override
    public void convert(Key k, Value v, Consumer<ByteSequence> consumer) {
      ByteSequence vis = k.getColumnVisibilityData();

      if (vis.length() > 0) {
        Set<ByteSequence> auths = cache.get(vis);
        if (auths == null) {
          auths = findAuths(vis);
          cache.put(new ArrayByteSequence(vis), auths);
        }

        for (ByteSequence auth : auths) {
          consumer.accept(auth);
        }
      }
    }

    private Set<ByteSequence> findAuths(ByteSequence vis) {
      HashSet<ByteSequence> auths = new HashSet<>();
      byte[] expression = vis.toArray();
      Node root = new ColumnVisibility(expression).getParseTree();

      findAuths(root, expression, auths);

      return auths;
    }

    private void findAuths(Node node, byte[] expression, HashSet<ByteSequence> auths) {
      switch (node.getType()) {
        case AND:
        case OR:
          for (Node child : node.getChildren()) {
            findAuths(child, expression, auths);
          }
          break;
        case TERM:
          auths.add(node.getTerm(expression));
          break;
        case EMPTY:
          break;
        default:
          throw new IllegalArgumentException("Unknown node type " + node.getType());
      }
    }
  }
}
