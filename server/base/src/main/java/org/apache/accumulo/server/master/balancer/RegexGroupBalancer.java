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

package org.apache.accumulo.server.master.balancer;

import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.hadoop.io.Text;

/**
 * A {@link GroupBalancer} that groups tablets using a configurable regex. To use this balancer configure the following settings for your table then configure
 * this balancer for your table.
 *
 * <ul>
 * <li>Set {@code table.custom.balancer.group.regex.pattern} to a regular expression. This regular expression must have one group. The regex is applied to the
 * tablet end row and whatever the regex group matches is used as the group. For example with a regex of {@code (\d\d).*} and an end row of {@code 12abc}, the
 * group for the tablet would be {@code 12}.
 * <li>Set {@code table.custom.balancer.group.regex.default} to a default group. This group is returned for the last tablet in the table and tablets for which
 * the regex does not match.
 * <li>Optionally set {@code table.custom.balancer.group.regex.wait.time} to time (can use time suffixes). This determines how long to wait between balancing.
 * Since this balancer scans the metadata table, may want to set this higher for large tables.
 * </ul>
 */

public class RegexGroupBalancer extends GroupBalancer {

  public static final String REGEX_PROPERTY = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "balancer.group.regex.pattern";
  public static final String DEFAUT_GROUP_PROPERTY = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "balancer.group.regex.default";
  public static final String WAIT_TIME_PROPERTY = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "balancer.group.regex.wait.time";

  private final String tableId;

  public RegexGroupBalancer(String tableId) {
    super(tableId);
    this.tableId = tableId;
  }

  @Override
  protected long getWaitTime() {
    Map<String,String> customProps = configuration.getTableConfiguration(tableId).getAllPropertiesWithPrefix(Property.TABLE_ARBITRARY_PROP_PREFIX);
    if (customProps.containsKey(WAIT_TIME_PROPERTY)) {
      return ConfigurationTypeHelper.getTimeInMillis(customProps.get(WAIT_TIME_PROPERTY));
    }

    return super.getWaitTime();
  }

  @Override
  protected Function<KeyExtent,String> getPartitioner() {

    Map<String,String> customProps = configuration.getTableConfiguration(tableId).getAllPropertiesWithPrefix(Property.TABLE_ARBITRARY_PROP_PREFIX);
    String regex = customProps.get(REGEX_PROPERTY);
    final String defaultGroup = customProps.get(DEFAUT_GROUP_PROPERTY);

    final Pattern pattern = Pattern.compile(regex);

    return new Function<KeyExtent,String>() {

      @Override
      public String apply(KeyExtent input) {
        Text er = input.getEndRow();
        if (er == null) {
          return defaultGroup;
        }

        Matcher matcher = pattern.matcher(er.toString());
        if (matcher.matches() && matcher.groupCount() == 1) {
          return matcher.group(1);
        }

        return defaultGroup;
      }
    };
  }
}
