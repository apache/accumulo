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
package org.apache.accumulo.examples.simple.filedata;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * An InputFormat that turns the file data ingested with {@link FileDataIngest} into an InputStream using {@link ChunkInputStream}. Mappers used with this
 * InputFormat must close the InputStream.
 */
public class ChunkInputFormat extends InputFormatBase<List<Entry<Key,Value>>,InputStream> {
  @Override
  public RecordReader<List<Entry<Key,Value>>,InputStream> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    return new RecordReaderBase<List<Entry<Key,Value>>,InputStream>() {
      private PeekingIterator<Entry<Key,Value>> peekingScannerIterator;

      @Override
      public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {
        super.initialize(inSplit, attempt);
        peekingScannerIterator = new PeekingIterator<Entry<Key,Value>>(scannerIterator);
        currentK = new ArrayList<Entry<Key,Value>>();
        currentV = new ChunkInputStream();
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        currentK.clear();
        if (peekingScannerIterator.hasNext()) {
          ++numKeysRead;
          Entry<Key,Value> entry = peekingScannerIterator.peek();
          while (!entry.getKey().getColumnFamily().equals(FileDataIngest.CHUNK_CF)) {
            currentK.add(entry);
            peekingScannerIterator.next();
            if (!peekingScannerIterator.hasNext())
              return true;
            entry = peekingScannerIterator.peek();
          }
          currentKey = entry.getKey();
          ((ChunkInputStream) currentV).setSource(peekingScannerIterator);
          if (log.isTraceEnabled())
            log.trace("Processing key/value pair: " + DefaultFormatter.formatEntry(entry, true));
          return true;
        }
        return false;
      }
    };
  }
}
