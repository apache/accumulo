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
package org.apache.accumulo.server.metadata.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;

public class AnyLocationIterator extends WrappingIterator {
  private static final Collection<ByteSequence> LOC_FAMS = List.of(
      new ArrayByteSequence(MetadataSchema.TabletsSection.FutureLocationColumnFamily.STR_NAME),
      new ArrayByteSequence(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.STR_NAME));

  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    Range r = new Range(range.getStartKey().getRow());
    // TODO check row is not sensible
    super.seek(r, LOC_FAMS, true);
  }
}
