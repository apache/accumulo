/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.sample.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.io.IOException;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.client.sample.Sampler;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamplerFactory {
  private static final Logger log = LoggerFactory.getLogger(SamplerFactory.class);

  public static Sampler newSampler(SamplerConfigurationImpl config, AccumuloConfiguration acuconf,
      boolean useAccumuloStart) {
    String context = ClassLoaderUtil.tableContext(acuconf);

    Class<? extends Sampler> clazz;
    try {
      if (!useAccumuloStart)
        clazz = SamplerFactory.class.getClassLoader().loadClass(config.getClassName())
            .asSubclass(Sampler.class);
      else
        clazz = ClassLoaderUtil.loadClass(context, config.getClassName(), Sampler.class);

      Sampler sampler = clazz.getDeclaredConstructor().newInstance();
      for (String option : config.getOptions().keySet()) {
        requireNonNull(option, option + " not specified");
        checkArgument(sampler.isValidOption(option), "Unknown option : %s", option);
      }
      sampler.init(config.toSamplerConfiguration());

      return sampler;

    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    } catch (IllegalArgumentException | NullPointerException e) {
      log.error("Cannot init sampler {}", e.getMessage());
      return null;
    }
  }

  public static Sampler newSampler(SamplerConfigurationImpl config, AccumuloConfiguration acuconf)
      throws IOException {
    return newSampler(config, acuconf, true);
  }
}
