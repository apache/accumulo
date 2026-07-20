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
package org.apache.accumulo.core.fate.user;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.WrappingIterator;

import com.google.common.base.Preconditions;

/**
 * Iterator is used by conditional mutations to check if row exists.
 */
public class RowExistsIterator extends WrappingIterator {

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    Preconditions.checkState(range.getStartKey() != null && range.getEndKey() != null);
    var startRow = range.getStartKey().getRow();
    var endRow = range.getEndKey().getRow();
    Preconditions.checkState(startRow.equals(endRow));
    Range r = new Range(startRow);
    super.seek(r, Set.of(), false);
  }
}
