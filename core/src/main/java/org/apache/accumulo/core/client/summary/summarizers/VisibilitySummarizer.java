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
package org.apache.accumulo.core.client.summary.summarizers;

import java.util.function.UnaryOperator;

import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.summary.CountingSummarizer;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;

/**
 * Counts column visibility labels. Leverages super class to defend against too many. This class is
 * useful for discovering what column visibilities are present when the expected number of
 * visibilities is small.
 *
 * @since 2.0.0
 *
 * @see AuthorizationSummarizer
 * @see TableOperations#addSummarizers(String,
 *      org.apache.accumulo.core.client.summary.SummarizerConfiguration...)
 * @see TableOperations#summaries(String)
 */
public class VisibilitySummarizer extends CountingSummarizer<ByteSequence> {

  @Override
  protected UnaryOperator<ByteSequence> copier() {
    return ArrayByteSequence::new;
  }

  @Override
  protected Converter<ByteSequence> converter() {
    return (k, v, c) -> c.accept(k.getColumnVisibilityData());
  }
}
