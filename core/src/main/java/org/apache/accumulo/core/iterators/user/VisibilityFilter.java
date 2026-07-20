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
package org.apache.accumulo.core.iterators.user;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.access.InvalidAccessExpressionException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.clientImpl.access.BytesAccess;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.map.LRUMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SortedKeyValueIterator that filters based on ColumnVisibility.
 */
public class VisibilityFilter extends Filter implements OptionDescriber {

  private static final Logger log = LoggerFactory.getLogger(VisibilityFilter.class);
  private static final BytesAccess.BytesEvaluator EMPTY_EVALUATOR =
      BytesAccess.newEvaluator(Authorizations.EMPTY);

  private BytesAccess.BytesEvaluator accessEvaluator;
  protected Map<ByteSequence,Boolean> cache;
  private final ArrayByteSequence testVis = new ArrayByteSequence(new byte[0]);

  private static final String AUTHS = "auths";
  private static final String FILTER_INVALID_ONLY = "filterInvalid";

  private boolean filterInvalid;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    validateOptions(options);
    this.filterInvalid = Boolean.parseBoolean(options.get(FILTER_INVALID_ONLY));

    if (!filterInvalid) {
      String auths = options.get(AUTHS);
      if (auths == null || auths.isEmpty()) {
        this.accessEvaluator = EMPTY_EVALUATOR;
      } else if (!auths.startsWith(Authorizations.HEADER)) {
        // the old serialization format does not support multiple auth sets, so treat the whole thing as one set
        this.accessEvaluator = BytesAccess.newEvaluator(new Authorizations(auths.getBytes(UTF_8)));
      } else {
        String[] authParts = auths.split(Authorizations.HEADER);
        if (authParts.length == 0) {
          this.accessEvaluator = EMPTY_EVALUATOR;
        } else {
          Collection<Authorizations> authSet = new ArrayList<>();
          for (int i = 0; i < authParts.length; i++) {
            String part = authParts[i];
            if (part.isEmpty()) {
              continue;
            } else {
              // split removes the HEADER, need to add it back
              String serializedAuthString = Authorizations.HEADER + authParts[i];
              authSet.add(new Authorizations(serializedAuthString.getBytes(UTF_8)));
            }
          }
          this.accessEvaluator = BytesAccess.newEvaluator(authSet);
        }
      }
    }
    this.cache = new LRUMap<>(1000);
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    VisibilityFilter result = (VisibilityFilter) super.deepCopy(env);
    result.filterInvalid = this.filterInvalid;
    result.accessEvaluator = this.accessEvaluator;
    result.cache = this.cache;
    return result;
  }

  @Override
  public boolean accept(Key k, Value v) {
    // The following call will replace the contents of testVis
    // with the bytes for the column visibility for k. Any cached
    // version of testVis needs to be a copy to avoid modifying
    // the cached version.
    k.getColumnVisibilityData(testVis);
    if (filterInvalid) {
      Boolean b = cache.get(testVis);
      if (b != null) {
        return b;
      }
      final ArrayByteSequence copy = new ArrayByteSequence(testVis);
      try {
        BytesAccess.validate(copy.toArray());
        // cache a copy of testVis
        cache.put(copy, true);
        return true;
      } catch (InvalidAccessExpressionException e) {
        // cache a copy of testVis
        cache.put(copy, false);
        return false;
      }
    } else {
      if (testVis.length() == 0) {
        return true;
      }

      Boolean b = cache.get(testVis);
      if (b != null) {
        return b;
      }

      final ArrayByteSequence copy = new ArrayByteSequence(testVis);
      try {
        boolean bb = accessEvaluator.canAccess(copy.toArray());
        // cache a copy of testVis
        cache.put(copy, bb);
        return bb;
      } catch (InvalidAccessExpressionException e) {
        log.error("Parse Error with visibility of Key: {}", k, e);
        return false;
      }
    }
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("visibilityFilter");
    io.setDescription("The VisibilityFilter allows you to filter for key/value"
        + " pairs by a set(s) of authorizations or filter invalid labels from corrupt files.");
    io.addNamedOption(FILTER_INVALID_ONLY,
        "if 'true', the iterator is instructed to ignore the authorizations and"
            + " only filter invalid visibility labels (default: false)");
    io.addNamedOption(AUTHS, "The set of Authorizations to use when filtering (default: empty"
        + " string, accepts only entries visible by all). The value can"
        + " either be of the older form (\"auth1,auth2\") which only supports"
        + " a single Authorizations object, or the newer form which uses " + Authorizations.HEADER
        + " concatenated with Base64 encoded comma-separated authorization tokens."
        + "The latter case supports one or more Authorizations (\"" + Authorizations.HEADER
        + "Base64(auth1,auth2)\") or (\"" + Authorizations.HEADER + "Base64(auth1,auth2)"
        + Authorizations.HEADER + "auth2,auth3\")");
    return io;
  }

  public static void setAuthorizations(IteratorSetting setting, Authorizations auths) {
    setting.addOption(AUTHS, auths.serialize());
  }

  public static void setAuthorizations(IteratorSetting setting, Collection<Authorizations> auths) {
    StringBuilder builder = new StringBuilder();
    auths.forEach(a -> builder.append(a.serialize()));
    setting.addOption(AUTHS, builder.toString());
  }

  public static void filterInvalidLabelsOnly(IteratorSetting setting, boolean featureEnabled) {
    setting.addOption(FILTER_INVALID_ONLY, Boolean.toString(featureEnabled));
  }

}
