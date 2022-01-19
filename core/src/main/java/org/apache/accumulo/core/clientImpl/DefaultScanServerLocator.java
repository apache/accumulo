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
package org.apache.accumulo.core.clientImpl;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.spi.scan.ScanServerLocator;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;

public class DefaultScanServerLocator implements ScanServerLocator {

  private final byte[] id = UUID.randomUUID().toString().getBytes(UTF_8);
  private ClientContext context = null;
  private ZooReaderWriter zrw = null;
  private String root = null;
  private String unreservedRoot = null;
  private String reservedRoot = null;

  @Override
  public void setClientContext(ClientContext ctx) {
    this.context = ctx;
    this.zrw = new ZooReaderWriter(this.context.getConfiguration());
    this.root = this.context.getZooKeeperRoot() + Constants.ZSSERVERS;
    this.unreservedRoot = this.root + "/unreserved/";
    this.reservedRoot = this.root + "/reserved/";
  }

  @Override
  public String reserveScanServer(KeyExtent extent)
      throws NoAvailableScanServerException, KeeperException, InterruptedException {
    for (String child : zrw.getChildren(unreservedRoot, null)) {
      try {
        zrw.putEphemeralData(this.reservedRoot + child, id);
      } catch (KeeperException e) {
        if (e.code().equals(Code.NODEEXISTS)) {
          continue;
        } else {
          throw e;
        }
      }
      zrw.delete(this.unreservedRoot + child); // TODO: in the first instance this is deleting an
      // ephemeral node created by the ScanServer. will that work?
      return child;
    }
    throw new NoAvailableScanServerException();
  }

  @Override
  public void unreserveScanServer(String hostPort) throws KeeperException, InterruptedException {
    try {
      byte[] data = zrw.getData(this.reservedRoot + hostPort);
      if (Arrays.equals(id, data)) {
        zrw.delete(this.reservedRoot + hostPort);
        zrw.putEphemeralData(this.unreservedRoot + hostPort, new byte[0]);
      } else {
        throw new RuntimeException("Attempting to unreserve a node that we didn't create.");
      }
    } catch (KeeperException e) {
      if (e.code().equals(Code.NONODE)) {
        throw new RuntimeException("Attempting to unreserve a node that does not exist");
      }
    }
  }

}
