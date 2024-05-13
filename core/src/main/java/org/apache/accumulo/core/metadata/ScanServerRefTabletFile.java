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
package org.apache.accumulo.core.metadata;

import java.net.URI;
import java.util.Objects;
import java.util.UUID;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class ScanServerRefTabletFile extends TabletFile {

  private final Value NULL_VALUE = new Value(new byte[0]);
  private final Text colf;
  private final String uuid;

  public ScanServerRefTabletFile(UUID serverLockUUID, String serverAddress, String file) {
    super(new Path(URI.create(file)));
    this.colf = new Text(serverAddress);
    this.uuid = serverLockUUID.toString();
  }

  public ScanServerRefTabletFile(String uuid, Text colf, Text file) {
    super(new Path(URI.create(file.toString())));
    this.colf = colf;
    this.uuid = uuid;
  }

  public String getRowSuffix() {
    return this.uuid;
  }

  public Text getFilePath() {
    return new Text(getPath().toString());
  }

  public Text getServerAddress() {
    return this.colf;
  }

  public Value getValue() {
    return NULL_VALUE;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((colf == null) ? 0 : colf.hashCode());
    result = prime * result + ((this.getRowSuffix() == null) ? 0 : this.getRowSuffix().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ScanServerRefTabletFile other = (ScanServerRefTabletFile) obj;
    return Objects.equals(colf, other.colf) && Objects.equals(uuid, other.uuid);
  }

  @Override
  public String toString() {
    return "ScanServerRefTabletFile [file=" + this.getPath().toString() + ", server address=" + colf
        + ", server lock uuid=" + this.uuid + "]";
  }

}
