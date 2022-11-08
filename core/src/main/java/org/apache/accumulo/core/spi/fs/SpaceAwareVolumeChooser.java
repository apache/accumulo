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
package org.apache.accumulo.core.spi.fs;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * A {@link PreferredVolumeChooser} that takes remaining HDFS space into account when making a
 * volume choice rather than a simpler round robin. The list of volumes to use can be limited using
 * the same properties as {@link PreferredVolumeChooser}
 *
 * @since 2.1.0
 */
public class SpaceAwareVolumeChooser extends PreferredVolumeChooser {

  private static final SecureRandom random = new SecureRandom();

  public static final String RECOMPUTE_INTERVAL = "spaceaware.volume.chooser.recompute.interval";

  // Default time to wait in ms. Defaults to 5 min
  private long defaultComputationCacheDuration = 300000;
  private LoadingCache<Set<String>,WeightedRandomCollection> choiceCache = null;

  private static final Logger log = LoggerFactory.getLogger(SpaceAwareVolumeChooser.class);

  private Configuration conf = new Configuration();

  protected double getFreeSpace(String uri) throws IOException {
    FileSystem pathFs = new Path(uri).getFileSystem(conf);
    FsStatus optionStatus = pathFs.getStatus();
    return ((double) optionStatus.getRemaining() / optionStatus.getCapacity());
  }

  @Override
  public String choose(VolumeChooserEnvironment env, Set<String> options) {
    try {
      return getCache(env).get(getPreferredVolumes(env, options)).next();
    } catch (ExecutionException e) {
      throw new IllegalStateException("Execution exception when attempting to cache choice", e);
    }
  }

  private synchronized LoadingCache<Set<String>,WeightedRandomCollection>
      getCache(VolumeChooserEnvironment env) {

    if (choiceCache == null) {
      String propertyValue = env.getServiceEnv().getConfiguration().getCustom(RECOMPUTE_INTERVAL);

      long computationCacheDuration = StringUtils.isNotBlank(propertyValue)
          ? Long.parseLong(propertyValue) : defaultComputationCacheDuration;

      choiceCache = CacheBuilder.newBuilder()
          .expireAfterWrite(computationCacheDuration, MILLISECONDS).build(new CacheLoader<>() {
            @Override
            public WeightedRandomCollection load(Set<String> key) {
              return new WeightedRandomCollection(key, env);
            }
          });
    }

    return choiceCache;
  }

  private class WeightedRandomCollection {
    private final NavigableMap<Double,String> map = new TreeMap<>();
    private double total = 0;

    private WeightedRandomCollection(Set<String> options, VolumeChooserEnvironment env) {

      if (options.size() < 1) {
        throw new IllegalStateException("Options was empty! No valid volumes to choose from.");
      }

      // Compute percentage space available on each volume
      for (String option : options) {
        try {
          double percentFree = getFreeSpace(option);
          add(percentFree, option);
        } catch (IOException e) {
          log.error("Unable to get file system status for" + option, e);
        }
      }

      if (map.size() < 1) {
        throw new IllegalStateException(
            "Weighted options was empty! Could indicate an issue getting file system status or "
                + "no free space on any volume");
      }
    }

    public WeightedRandomCollection add(double weight, String result) {
      if (weight <= 0) {
        log.info("Weight was 0. Not adding " + result);
        return this;
      }
      total += weight;
      map.put(total, result);
      return this;
    }

    public String next() {
      double value = random.nextDouble() * total;
      return map.higherEntry(value).getValue();
    }
  }
}
