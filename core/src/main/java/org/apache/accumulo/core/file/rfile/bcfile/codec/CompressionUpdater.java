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
package org.apache.accumulo.core.file.rfile.bcfile.codec;

import jline.internal.Log;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.rfile.bcfile.Compression;
import org.apache.log4j.Logger;

/**
 * Runnable that will be used to update the compression class.
 *
 */
public class CompressionUpdater implements Runnable {

  private static final Logger LOG = Logger.getLogger(CompressorFactory.class);
  /**
   * Compressor factory class
   */
  private Class<? extends CompressorFactory> compressorFactoryClazz = CompressorFactory.class;

  private CompressorFactory currentInstance = null;

  /**
   * Accumulo configuration.
   */
  private AccumuloConfiguration acuConf;

  public CompressionUpdater(AccumuloConfiguration acuConf) {
    this.acuConf = acuConf;
    currentInstance = new CompressorFactory(acuConf);
    Compression.setCompressionFactory(currentInstance);
  }

  @Override
  public void run() {
    final String compressorClass = acuConf.get(Property.TSERV_COMPRESSOR_FACTORY);
    if (!compressorClass.equals(compressorFactoryClazz.getCanonicalName())) {
      Class<? extends CompressorFactory> tempFactory = null;
      try {
        tempFactory = Class.forName(compressorClass).asSubclass(CompressorFactory.class);
      } catch (ClassNotFoundException cfe) {
        Log.warn("Could not find class " + compressorClass + " so not setting desired CompressorFactory");
        // do nothing
        return;
      }
      LOG.info("Setting compressor factory to " + tempFactory);
      try {
        Compression.setCompressionFactory(tempFactory.getConstructor(AccumuloConfiguration.class).newInstance(acuConf));
        compressorFactoryClazz = tempFactory;
      } catch (Exception e) {
        LOG.error("Could not set compressor factory to " + compressorFactoryClazz + " defaulting to CompressorFactory", e);
        Compression.setCompressionFactory(new CompressorFactory(acuConf));
      }
    } else {
      currentInstance.update(acuConf);
    }

    /**
     * Adjust compression buffer sizes.
     */
    final long inputBufferSize = acuConf.getMemoryInBytes(Property.TSERV_COMPRESSOR_IN_BUFFER);
    Compression.setDataInputBufferSize((int) inputBufferSize);
    final long outputBufferSize = acuConf.getMemoryInBytes(Property.TSERV_COMPRESSOR_OUT_BUFFER);
    Compression.setDataOutputBufferSize((int) outputBufferSize);
  }

}
