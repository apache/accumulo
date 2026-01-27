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
package org.apache.accumulo.core.conf;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeprecatedPropertyUtil {

  public static class PropertyRenamer {
    final Predicate<String> keyFilter;
    final UnaryOperator<String> keyMapper;

    public PropertyRenamer(Predicate<String> keyFilter, UnaryOperator<String> keyMapper) {
      this.keyFilter = requireNonNull(keyFilter);
      this.keyMapper = requireNonNull(keyMapper);
    }

    public static PropertyRenamer renamePrefix(String oldPrefix, String newPrefix) {
      return new PropertyRenamer(p -> p.startsWith(oldPrefix),
          p -> newPrefix + p.substring(oldPrefix.length()));
    }
  }

  private static final Logger log = LoggerFactory.getLogger(DeprecatedPropertyUtil.class);

  /**
   * Ordered list of renamers
   */
  protected static final List<PropertyRenamer> renamers = new ArrayList<>();

  /**
   * Checks if {@code propertyName} is a deprecated property name and return its replacement name,
   * if one is available, or the original name if no replacement is available. If a property has a
   * replacement that itself was replaced, this method will return the final recommended property,
   * after processing each replacement in order. If the final name has changed from the original
   * name, the logging action is triggered with a provided logger, the original name, and the
   * replacement name.
   * <p>
   * This is expected to be used only with system properties stored in the SiteConfiguration and
   * ZooConfiguration, and not for per-table or per-namespace configuration in ZooKeeper.
   *
   * @param propertyName the name of the potentially deprecated property to check for a replacement
   *        name
   * @param loggerActionOnReplace the action to execute, if not null, if a replacement name was
   *        found
   * @return either the replacement for {@code propertyName}, or {@code propertyName} if the
   *         property is not deprecated
   */
  public static String getReplacementName(final String propertyName,
      BiConsumer<Logger,String> loggerActionOnReplace) {
    String replacement = requireNonNull(propertyName);
    requireNonNull(loggerActionOnReplace);
    for (PropertyRenamer renamer : renamers) {
      if (renamer.keyFilter.test(replacement)) {
        replacement = renamer.keyMapper.apply(replacement);
      }
    }
    // perform the logger action if the property was replaced
    if (!replacement.equals(propertyName)) {
      loggerActionOnReplace.accept(log, replacement);
    }
    return replacement;
  }

}
