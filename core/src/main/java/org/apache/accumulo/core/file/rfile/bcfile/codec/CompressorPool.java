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
package org.apache.accumulo.core.file.rfile.bcfile.codec;

import java.io.IOException;
import java.util.Objects;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.rfile.bcfile.Compression.Algorithm;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compressor factory extension that enables object pooling using Commons Pool. The design will have
 * a keyed compressor pool and decompressor pool. The key of which will be the Algorithm itself.
 *
 */
public class CompressorPool extends DefaultCompressorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(CompressorPoolFactory.class);

  /**
   * Compressor pool.
   */
  GenericKeyedObjectPool<Algorithm,Compressor> compressorPool;

  /**
   * Decompressor pool
   */
  GenericKeyedObjectPool<Algorithm,Decompressor> decompressorPool;

  public CompressorPool(AccumuloConfiguration acuConf) {

    super(acuConf);

    compressorPool = new GenericKeyedObjectPool<Algorithm,Compressor>(new CompressorPoolFactory());
    // ensure that the pool grows when needed

    compressorPool.setBlockWhenExhausted(false);
    // no limit
    compressorPool.setMaxTotal(-1);
    compressorPool.setMaxTotalPerKey(-1);
    compressorPool.setTestOnReturn(false);

    decompressorPool =
        new GenericKeyedObjectPool<Algorithm,Decompressor>(new DecompressorPoolFactory());
    // ensure that the pool grows when needed.
    decompressorPool.setBlockWhenExhausted(false);
    // no limit
    decompressorPool.setMaxTotal(-1);
    decompressorPool.setMaxTotalPerKey(-1);
    decompressorPool.setTestOnReturn(false);
    // perform the initial update.
    update(acuConf);

  }

  /**
   * Set the max idle time that the compressor and decompressor pools will hold objects.
   *
   * @param maxIdle
   *          maximum idle time.
   */
  public void setMaxIdle(final int maxIdle) {
    // check that we are changing the value.
    // this will avoid synchronization within the pool
    if (maxIdle != compressorPool.getMaxIdlePerKey())
      compressorPool.setMaxIdlePerKey(maxIdle);
    if (maxIdle != decompressorPool.getMaxIdlePerKey())
      decompressorPool.setMaxIdlePerKey(maxIdle);
  }

  @Override
  public Compressor getCompressor(Algorithm compressionAlgorithm) throws IOException {
    Objects.requireNonNull(compressionAlgorithm, "Algorithm cannot be null");
    try {
      return compressorPool.borrowObject(compressionAlgorithm);
    } catch (Exception e) {
      // could not borrow the object, therefore we will attempt to create it
      // this will most likely result in an exception when returning so an end will occur
      LOG.warn("Could not borrow compressor; creating instead", e);
      return compressionAlgorithm.getCodec().createCompressor();
    }
  }

  @Override
  public void releaseCompressor(Algorithm compressionAlgorithm, Compressor compressor) {
    Objects.requireNonNull(compressionAlgorithm, "Algorithm cannot be null");
    Objects.requireNonNull(compressor, "Compressor should not be null");
    try {
      compressorPool.returnObject(compressionAlgorithm, compressor);
    } catch (Exception e) {
      LOG.warn("Could not return compressor; closing instead", e);
      // compressor failed to be returned. Let's free the memory associated with it
      compressor.end();
    }

  }

  @Override
  public void releaseDecompressor(Algorithm compressionAlgorithm, Decompressor decompressor) {
    Objects.requireNonNull(compressionAlgorithm, "Algorithm cannot be null");
    Objects.requireNonNull(decompressor, "Deompressor should not be null");
    try {
      decompressorPool.returnObject(compressionAlgorithm, decompressor);
    } catch (Exception e) {
      LOG.warn("Could not return decompressor; closing instead", e);
      // compressor failed to be returned. Let's free the memory associated with it
      decompressor.end();
    }

  }

  @Override
  public Decompressor getDecompressor(Algorithm compressionAlgorithm) {
    Objects.requireNonNull(compressionAlgorithm, "Algorithm cannot be null");
    try {
      return decompressorPool.borrowObject(compressionAlgorithm);
    } catch (Exception e) {
      LOG.warn("Could not borrow decompressor; creating instead", e);
      // could not borrow the object, therefore we will attempt to create it
      // this will most likely result in an exception when returning so an end will occur
      return compressionAlgorithm.getCodec().createDecompressor();
    }
  }

  /**
   * Closes both pools, which will clear and evict the respective compressor/decompressors.
   * {@inheritDoc}
   */
  @Override
  public void close() {
    try {
      compressorPool.close();
    } catch (Exception e) {
      LOG.error("Exception while closing compressor pool", e);
    }
    try {
      decompressorPool.close();
    } catch (Exception e) {
      LOG.error("Exception while closing decompressor pool", e);
    }

  }

  /**
   * Updates the maximum number of idle objects allowed, the sweep time, and the minimum time before
   * eviction is used {@inheritDoc}
   */
  @Override
  public void update(final AccumuloConfiguration acuConf) {
    try {
      final int poolMaxIdle = acuConf.getCount(Property.TSERV_COMPRESSOR_POOL_IDLE);
      setMaxIdle(poolMaxIdle);

      final long idleSweepTimeMs =
          acuConf.getTimeInMillis(Property.TSERV_COMPRESSOR_POOL_IDLE_SWEEP_TIME);

      setIdleSweepTime(idleSweepTimeMs);
      final long idleStoreTimeMs =
          acuConf.getTimeInMillis(Property.TSERV_COMPRESSOR_POOL_IDLE_STORE_TIME);
      setIdleStoreTime(idleStoreTimeMs);

    } catch (Exception e) {
      LOG.error("Invalid compressor pool configuration", e);
    }
  }

  /**
   * Sets the minimum amount of time may pass before a (de)compressor may be evicted.
   *
   * @param idleStoreTimeMs
   *          minimum time in ms before a (de)compressor is considered for eviction.
   */
  public void setIdleStoreTime(final long idleStoreTimeMs) {

    if (idleStoreTimeMs > 0) {
      // if > 0, then we check that we aren't setting it to the same value
      // we used previously. If so, we call the setter, from which a thread
      // will be launched.
      if (compressorPool.getMinEvictableIdleTimeMillis() != idleStoreTimeMs) {

        compressorPool.setMinEvictableIdleTimeMillis(idleStoreTimeMs);
      }

      if (decompressorPool.getMinEvictableIdleTimeMillis() != idleStoreTimeMs) {
        decompressorPool.setMinEvictableIdleTimeMillis(idleStoreTimeMs);
      }
    } else {
      if (compressorPool.getMinEvictableIdleTimeMillis() > 0) {
        compressorPool.setMinEvictableIdleTimeMillis(-1);
      }

      if (decompressorPool.getMinEvictableIdleTimeMillis() > 0) {
        decompressorPool.setMinEvictableIdleTimeMillis(-1);
      }
    }
  }

  /**
   * Sets the idle sweep time if &gt; 0.
   *
   * @param idleSweepTimeMs
   *          idle sweep time.
   */
  public void setIdleSweepTime(final long idleSweepTimeMs) {
    if (idleSweepTimeMs > 0) {
      // if > 0, then we check that we aren't setting it to the same value
      // we used previously. If so, we call the setter, from which a thread
      // will be launched.
      if (compressorPool.getTimeBetweenEvictionRunsMillis() != idleSweepTimeMs) {

        compressorPool.setTimeBetweenEvictionRunsMillis(idleSweepTimeMs);
      }

      if (decompressorPool.getTimeBetweenEvictionRunsMillis() != idleSweepTimeMs) {
        decompressorPool.setTimeBetweenEvictionRunsMillis(idleSweepTimeMs);
      }
    } else {
      if (compressorPool.getTimeBetweenEvictionRunsMillis() > 0) {
        compressorPool.setTimeBetweenEvictionRunsMillis(-1);
      }

      if (decompressorPool.getTimeBetweenEvictionRunsMillis() > 0) {
        decompressorPool.setTimeBetweenEvictionRunsMillis(-1);
      }

    }

  }
}
