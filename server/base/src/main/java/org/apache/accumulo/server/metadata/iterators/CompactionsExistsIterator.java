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
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.hadoop.io.Text;

public class CompactionsExistsIterator extends WrappingIterator {
  private static final Collection<ByteSequence> FAMS =
      Set.of(new ArrayByteSequence(ExternalCompactionColumnFamily.STR_NAME));

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {

    Text tabletRow = LocationExistsIterator.getTabletRow(range);
    Key startKey = new Key(tabletRow, ExternalCompactionColumnFamily.NAME);
    Key endKey =
        new Key(tabletRow, ExternalCompactionColumnFamily.NAME).followingKey(PartialKey.ROW_COLFAM);

    Range r = new Range(startKey, true, endKey, false);

    super.seek(r, FAMS, true);
  }

}
