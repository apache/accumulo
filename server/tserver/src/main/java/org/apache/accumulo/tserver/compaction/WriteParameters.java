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
package org.apache.accumulo.tserver.compaction;

import static com.google.common.base.Preconditions.checkArgument;

@Deprecated(since = "2.1.0", forRemoval = true)
public class WriteParameters {
  private String compressType = null;
  private long hdfsBlockSize = 0;
  private long blockSize = 0;
  private long indexBlockSize = 0;
  private int replication = 0;

  public String getCompressType() {
    return compressType;
  }

  public void setCompressType(String compressType) {
    this.compressType = compressType;
  }

  public long getHdfsBlockSize() {
    return hdfsBlockSize;
  }

  public void setHdfsBlockSize(long hdfsBlockSize) {
    checkArgument(hdfsBlockSize >= 0);
    this.hdfsBlockSize = hdfsBlockSize;
  }

  public long getBlockSize() {
    return blockSize;
  }

  public void setBlockSize(long blockSize) {
    checkArgument(blockSize >= 0);
    this.blockSize = blockSize;
  }

  public long getIndexBlockSize() {
    return indexBlockSize;
  }

  public void setIndexBlockSize(long indexBlockSize) {
    checkArgument(indexBlockSize >= 0);
    this.indexBlockSize = indexBlockSize;
  }

  public int getReplication() {
    return replication;
  }

  public void setReplication(int replication) {
    checkArgument(replication >= 0);
    this.replication = replication;
  }
}
