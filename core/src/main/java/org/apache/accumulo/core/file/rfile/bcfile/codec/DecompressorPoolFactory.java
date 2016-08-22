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

import org.apache.accumulo.core.file.rfile.bcfile.Compression.Algorithm;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.io.compress.Decompressor;

/**
 * Factory pattern used to create decompressors within CompressorPool
 *
 */
public class DecompressorPoolFactory implements KeyedPooledObjectFactory<Algorithm,Decompressor> {

  @Override
  public PooledObject<Decompressor> makeObject(Algorithm key) throws Exception {
    return new DefaultPooledObject<Decompressor>(key.getCodec().createDecompressor());
  }

  @Override
  public void destroyObject(Algorithm algorithm, PooledObject<Decompressor> pooledObject) throws Exception {
    pooledObject.getObject().end();
  }

  @Override
  public boolean validateObject(Algorithm algorithm, PooledObject<Decompressor> pooledObject) {
    return pooledObject.getObject().finished();
  }

  @Override
  public void activateObject(Algorithm algorithm, PooledObject<Decompressor> pooledObject) throws Exception {
    pooledObject.getObject().reset();
  }

  @Override
  public void passivateObject(Algorithm algorithm, PooledObject<Decompressor> pooledObject) throws Exception {
    pooledObject.getObject().reset();
  }

}
