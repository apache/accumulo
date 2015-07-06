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
package org.apache.accumulo.core.client.impl;

import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.fate.AcceptableException;

/**
 * Concrete implementation of {@link AcceptableException} for table operations.
 */
public class AcceptableThriftTableOperationException extends ThriftTableOperationException implements AcceptableException {

  private static final long serialVersionUID = 1L;

  public AcceptableThriftTableOperationException(String tableId, String tableName, TableOperation op, TableOperationExceptionType type, String description) {
    super(tableId, tableName, op, type, description);
  }
}
