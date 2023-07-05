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
import java.util.Set;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

/**
 * A specialized iterator used for conditional mutations to check if a location is present in a
 * tablet.
 */
public class LocationExistsIterator extends WrappingIterator {
  private static final Collection<ByteSequence> LOC_FAMS =
      Set.of(new ArrayByteSequence(FutureLocationColumnFamily.STR_NAME),
          new ArrayByteSequence(CurrentLocationColumnFamily.STR_NAME));

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {

    Text tabletRow = getTabletRow(range);
    Key startKey = new Key(tabletRow, FutureLocationColumnFamily.NAME);
    Key endKey =
        new Key(tabletRow, CurrentLocationColumnFamily.NAME).followingKey(PartialKey.ROW_COLFAM);

    Range r = new Range(startKey, true, endKey, false);

    super.seek(r, LOC_FAMS, true);
  }

  static Text getTabletRow(Range range) {
    var row = range.getStartKey().getRow();
    // expecting this range to cover a single metadata row, so validate the range meets expectations
    TabletsSection.validateRow(row);
    Preconditions.checkArgument(row.equals(range.getEndKey().getRow()));
    return range.getStartKey().getRow();
  }
}
