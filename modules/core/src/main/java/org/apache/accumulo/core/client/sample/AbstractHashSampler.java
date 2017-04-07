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

package org.apache.accumulo.core.client.sample;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.sample.impl.DataoutputHasher;

import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

/**
 * A base class that can be used to create Samplers based on hashing. This class offers consistent options for configuring the hash function. The subclass
 * decides which parts of the key to hash.
 *
 * <p>
 * This class support two options passed into {@link #init(SamplerConfiguration)}. One option is {@code hasher} which specifies a hashing algorithm. Valid
 * values for this option are {@code md5}, {@code sha1}, and {@code murmur3_32}. If you are not sure, then choose {@code murmur3_32}.
 *
 * <p>
 * The second option is {@code modulus} which can have any positive integer as a value.
 *
 * <p>
 * Any data where {@code hash(data) % modulus == 0} will be selected for the sample.
 *
 * @since 1.8.0
 */

public abstract class AbstractHashSampler implements Sampler {

  private HashFunction hashFunction;
  private int modulus;

  private static final Set<String> VALID_OPTIONS = ImmutableSet.of("hasher", "modulus");

  /**
   * Subclasses with options should override this method and return true if the option is valid for the subclass or if {@code super.isValidOption(opt)} returns
   * true.
   */

  protected boolean isValidOption(String option) {
    return VALID_OPTIONS.contains(option);
  }

  /**
   * Subclasses with options should override this method and call {@code super.init(config)}.
   */

  @Override
  public void init(SamplerConfiguration config) {
    String hasherOpt = config.getOptions().get("hasher");
    String modulusOpt = config.getOptions().get("modulus");

    requireNonNull(hasherOpt, "Hasher not specified");
    requireNonNull(modulusOpt, "Modulus not specified");

    for (String option : config.getOptions().keySet()) {
      checkArgument(isValidOption(option), "Unknown option : %s", option);
    }

    switch (hasherOpt) {
      case "murmur3_32":
        hashFunction = Hashing.murmur3_32();
        break;
      case "md5":
        hashFunction = Hashing.md5();
        break;
      case "sha1":
        hashFunction = Hashing.sha1();
        break;
      default:
        throw new IllegalArgumentException("Uknown hahser " + hasherOpt);
    }

    modulus = Integer.parseInt(modulusOpt);
  }

  /**
   * Subclass must override this method and hash some portion of the key.
   *
   * @param hasher
   *          Data written to this will be used to compute the hash for the key.
   */
  protected abstract void hash(DataOutput hasher, Key k) throws IOException;

  @Override
  public boolean accept(Key k) {
    Hasher hasher = hashFunction.newHasher();
    try {
      hash(new DataoutputHasher(hasher), k);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return hasher.hash().asInt() % modulus == 0;
  }
}
