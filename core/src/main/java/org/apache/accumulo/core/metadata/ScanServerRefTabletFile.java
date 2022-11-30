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

import java.util.Objects;
import java.util.UUID;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class ScanServerRefTabletFile extends TabletFile {

  private final Value NULL_VALUE = new Value(new byte[0]);
  private final Text colf;
  private final Text colq;

  public ScanServerRefTabletFile(String file, String serverAddress, UUID serverLockUUID) {
    super(new Path(file));
    this.colf = new Text(serverAddress);
    this.colq = new Text(serverLockUUID.toString());
  }

  public ScanServerRefTabletFile(String file, Text colf, Text colq) {
    super(new Path(file));
    this.colf = colf;
    this.colq = colq;
  }

  public String getRowSuffix() {
    return this.getPathStr();
  }

  public Text getServerAddress() {
    return this.colf;
  }

  public Text getServerLockUUID() {
    return this.colq;
  }

  public Value getValue() {
    return NULL_VALUE;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((colf == null) ? 0 : colf.hashCode());
    result = prime * result + ((colq == null) ? 0 : colq.hashCode());
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
    return Objects.equals(colf, other.colf) && Objects.equals(colq, other.colq);
  }

  @Override
  public String toString() {
    return "ScanServerRefTabletFile [file=" + this.getRowSuffix() + ", server address=" + colf
        + ", server lock uuid=" + colq + "]";
  }

}
