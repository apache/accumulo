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
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.commons.collections4.map.LRUMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SortedKeyValueIterator that filters based on ColumnVisibility.
 */
public class VisibilityFilter extends Filter implements OptionDescriber {

  protected VisibilityEvaluator ve;
  protected Map<ByteSequence,Boolean> cache;

  private static final Logger log = LoggerFactory.getLogger(VisibilityFilter.class);

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
      Authorizations authObj = auths == null || auths.isEmpty() ? new Authorizations()
          : new Authorizations(auths.getBytes(UTF_8));
      this.ve = new VisibilityEvaluator(authObj);
    }
    this.cache = new LRUMap<>(1000);
  }

  @Override
  public boolean accept(Key k, Value v) {
    ByteSequence testVis = k.getColumnVisibilityData();
    if (filterInvalid) {
      Boolean b = cache.get(testVis);
      if (b != null) {
        return b;
      }
      try {
        new ColumnVisibility(testVis.toArray());
        cache.put(testVis, true);
        return true;
      } catch (BadArgumentException e) {
        cache.put(testVis, false);
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

      try {
        boolean bb = ve.evaluate(new ColumnVisibility(testVis.toArray()));
        cache.put(testVis, bb);
        return bb;
      } catch (VisibilityParseException | BadArgumentException e) {
        log.error("Parse Error", e);
        return false;
      }
    }
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("visibilityFilter");
    io.setDescription("The VisibilityFilter allows you to filter for key/value"
        + " pairs by a set of authorizations or filter invalid labels from corrupt files.");
    io.addNamedOption(FILTER_INVALID_ONLY,
        "if 'true', the iterator is instructed to ignore the authorizations and"
            + " only filter invalid visibility labels (default: false)");
    io.addNamedOption(AUTHS,
        "the serialized set of authorizations to filter against (default: empty"
            + " string, accepts only entries visible by all)");
    return io;
  }

  public static void setAuthorizations(IteratorSetting setting, Authorizations auths) {
    setting.addOption(AUTHS, auths.serialize());
  }

  public static void filterInvalidLabelsOnly(IteratorSetting setting, boolean featureEnabled) {
    setting.addOption(FILTER_INVALID_ONLY, Boolean.toString(featureEnabled));
  }

}
