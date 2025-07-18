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
package org.apache.accumulo.core.client.admin.compaction;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;

import com.google.common.base.Preconditions;

/**
 * <p>
 * A compaction configurer that extends CompressionConfigurer and adds the ability to control and
 * configure how Erasure Codes work. This plugin accepts the following options, when setting these
 * options on a table prefix them with {@code table.compaction.configurer.opts.}.
 *
 * <ul>
 * <li>Set {@value #ERASURE_CODE_SIZE} to a size in bytes. The suffixes K,M,and G can be used. This
 * is the minimum file size to allow conversion to erasure code. If a file is below this size it
 * will be replicated otherwise erasure coding will be enabled.
 * <li>Set {@value #BYPASS_ERASURE_CODES} to true or false. Setting this to true will bypass erasure
 * codes if the are configured. This options allows initiated compactions to bypass logic in this
 * class if needed.
 * <li>Optionally set {@value #ERASURE_CODE_POLICY} as the policy to use when erasure codes are
 * used. If this is not set, then it will fall back to what is set on the table.</li>
 * </ul>
 *
 * </p>
 *
 * @since 2.1.4
 */
public class ErasureCodeConfigurer extends CompressionConfigurer {
  public static final String ERASURE_CODE_SIZE = "erasure.code.size.conversion";
  public static final String BYPASS_ERASURE_CODES = "erasure.code.bypass";
  public static final String ERASURE_CODE_POLICY = "erasure.code.policy";
  private String ecPolicyName = null;
  private Long ecSize;
  private Boolean byPassEC = false;

  @Override
  public void init(InitParameters iparams) {
    var options = iparams.getOptions();

    this.ecSize =
        ConfigurationTypeHelper.getFixedMemoryAsBytes(options.getOrDefault(ERASURE_CODE_SIZE, "0"));
    this.ecPolicyName = options.get(ERASURE_CODE_POLICY);
    this.byPassEC = Boolean.parseBoolean(options.getOrDefault(BYPASS_ERASURE_CODES, "false"));

    if (ecSize == 0 && !byPassEC) {
      throw new IllegalArgumentException(
          "Must set either " + ERASURE_CODE_SIZE + " or " + BYPASS_ERASURE_CODES);
    }

    if (!byPassEC) {
      Preconditions.checkArgument(this.ecSize > 0,
          "Must set " + ERASURE_CODE_SIZE + " to a positive integer");
    }

    super.init(iparams);
  }

  @Override
  public Overrides override(InputParameters params) {
    Map<String,String> overs = new HashMap<>(super.override(params).getOverrides());
    if (this.byPassEC) {
      // Allow for user initiated compactions to pass an options to bypass EC.
      overs.put(Property.TABLE_ENABLE_ERASURE_CODES.getKey(), "disable");
    } else {
      long inputsSum =
          params.getInputFiles().stream().mapToLong(CompactableFile::getEstimatedSize).sum();
      if (inputsSum >= this.ecSize) {
        overs.put(Property.TABLE_ENABLE_ERASURE_CODES.getKey(), "enable");
        if (ecPolicyName != null) {
          overs.put(Property.TABLE_ERASURE_CODE_POLICY.getKey(), ecPolicyName);
        }
      } else {
        overs.put(Property.TABLE_ENABLE_ERASURE_CODES.getKey(), "disable");
      }
    }
    return new Overrides(overs);
  }
}
