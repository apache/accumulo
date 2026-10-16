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
package org.apache.accumulo.server.log;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment;
import org.apache.accumulo.core.spi.wal.WalProvider;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.fs.VolumeChooserEnvironmentImpl;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

/** Filesystem-backed WAL provider. */
public class DfsWalProvider implements WalProvider {

  /** Stable provider ID used for WALs stored on HDFS. */
  public static final String PROVIDER_ID = "dfs";

  private ServerContext context;

  @Override
  public String getId() {
    return PROVIDER_ID;
  }

  @Override
  public void init(ServiceEnvironment environment) {
    if (!(environment instanceof ServiceEnvironmentImpl serviceEnvironment)) {
      throw new IllegalArgumentException("DfsWalProvider requires the server service environment");
    }
    context = serviceEnvironment.getContext();
  }

  @Override
  public Writer create(String serverAddress, UUID id) throws IOException {
    String addressForFilename = serverAddress.replace(':', '+');
    var chooserEnv =
        new VolumeChooserEnvironmentImpl(VolumeChooserEnvironment.Scope.LOGGER, context);
    String logPath =
        context.getVolumeManager().choose(chooserEnv, context.getBaseUris()) + Path.SEPARATOR
            + Constants.WAL_DIR + Path.SEPARATOR + addressForFilename + Path.SEPARATOR + id;
    var path = new Path(logPath);
    VolumeManager fs = context.getVolumeManager();
    short replication = (short) context.getConfiguration().getCount(Property.TSERV_WAL_REPLICATION);
    if (replication == 0) {
      replication = fs.getDefaultReplication(path);
    }
    long blockSize = context.getConfiguration().getAsBytes(Property.TSERV_WAL_BLOCKSIZE);
    if (blockSize == 0) {
      blockSize = (long) (context.getConfiguration().getAsBytes(Property.TSERV_WAL_MAX_SIZE) * 1.1);
    }
    FSDataOutputStream output;
    if (context.getConfiguration().getBoolean(Property.TSERV_WAL_SYNC)) {
      output = fs.createSyncable(path, 0, replication, blockSize);
    } else {
      output = fs.create(path, true, 0, replication, blockSize);
    }
    try {
      output.setDropBehind(Boolean.TRUE);
    } catch (UnsupportedOperationException e) {
      // Not all filesystems support drop-behind.
    } catch (IOException e) {
      // Drop-behind is a performance hint, not a requirement.
    }
    if (!fs.canSyncAndFlush(path)) {
      org.slf4j.LoggerFactory.getLogger(DfsWalProvider.class)
          .warn("sync not supported for log file {}. Data loss may occur.", logPath);
    }
    return new DfsWriter(logPath, output);
  }

  @Override
  public InputStream open(String reference) throws IOException {
    return context.getVolumeManager().open(new Path(reference));
  }

  @Override
  public boolean exists(String reference) throws IOException {
    return context.getVolumeManager().exists(new Path(reference));
  }

  @Override
  public void delete(String reference) throws IOException {
    VolumeManager fs = context.getVolumeManager();
    Path path = new Path(reference);
    if (!fs.moveToTrash(path)) {
      fs.deleteRecursively(path);
    }
  }

  private static class DfsWriter implements Writer {
    private final String reference;
    private final FSDataOutputStream output;
    private boolean closed;

    DfsWriter(String reference, FSDataOutputStream output) {
      this.reference = reference;
      this.output = output;
    }

    @Override
    public String getReference() {
      return reference;
    }

    @Override
    public java.io.OutputStream getOutputStream() {
      return output;
    }

    @Override
    public void sync(Durability durability) throws IOException {
      if (durability == Durability.SYNC) {
        output.hsync();
      } else if (durability == Durability.FLUSH) {
        output.hflush();
      } else {
        throw new IllegalArgumentException("Unexpected WAL sync durability " + durability);
      }
    }

    @Override
    public long getLength() throws IOException {
      return output.getPos();
    }

    @Override
    public int getCurrentReplication() throws IOException {
      var wrapped = output.getWrappedStream();
      return wrapped instanceof DFSOutputStream dfs ? dfs.getCurrentBlockReplication() : -1;
    }

    @Override
    public String[] getPipeline() {
      var wrapped = output.getWrappedStream();
      if (wrapped instanceof DFSOutputStream dfs) {
        DatanodeInfo[] pipeline = dfs.getPipeline();
        String[] names = new String[pipeline.length];
        for (int i = 0; i < pipeline.length; i++) {
          names[i] = pipeline[i].getName();
        }
        return names;
      }
      return new String[0];
    }

    @Override
    public synchronized void close() throws IOException {
      if (!closed) {
        output.close();
        closed = true;
      }
    }
  }
}
