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
package org.apache.accumulo.core.iterators;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.SELECTED_COLUMN;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.SelectedFiles;

/**
 * This iterator encodes the value as a SelectedFiles object then decodes it back into a String to
 * ensure the resulting String value has a sorted array of file paths.
 */
public class SortedFilesIterator extends WrappingIterator {

  @Override
  public Value getTopValue() {
    // only re-sort the value if the current top key has the expected column
    if (SELECTED_COLUMN.hasColumns(super.getTopKey())) {
      Value unsortedValue = super.getTopValue();
      SelectedFiles selectedFiles = SelectedFiles.from(new String(unsortedValue.get(), UTF_8));
      return new Value(selectedFiles.getMetadataValue().getBytes(UTF_8));
    }
    return super.getTopValue();
  }

}
