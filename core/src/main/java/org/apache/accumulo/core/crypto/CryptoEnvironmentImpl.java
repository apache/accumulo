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
package org.apache.accumulo.core.crypto;

import java.util.Objects;
import java.util.Optional;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * @since 2.0
 */
public class CryptoEnvironmentImpl implements CryptoEnvironment {

  private final Scope scope;
  private final TableId tableId;
  private final byte[] decryptionParams;

  /**
   * Construct the crypto environment. The decryptionParams can be null.
   */
  public CryptoEnvironmentImpl(Scope scope, @Nullable TableId tableId,
      @Nullable byte[] decryptionParams) {
    this.scope = Objects.requireNonNull(scope);
    this.tableId = tableId;
    this.decryptionParams = decryptionParams;
  }

  public CryptoEnvironmentImpl(Scope scope) {
    this.scope = scope;
    this.tableId = null;
    this.decryptionParams = null;
  }

  @Override
  public Scope getScope() {
    return scope;
  }

  @Override
  public Optional<TableId> getTableId() {
    return Optional.ofNullable(tableId);
  }

  @Override
  public Optional<byte[]> getDecryptionParams() {
    return Optional.ofNullable(decryptionParams);
  }

  @Override
  public String toString() {
    String str = scope + " tableId=" + tableId + " decryptParams.length=";
    if (decryptionParams == null) {
      str += 0;
    } else {
      str += decryptionParams.length;
    }
    return str;
  }
}
