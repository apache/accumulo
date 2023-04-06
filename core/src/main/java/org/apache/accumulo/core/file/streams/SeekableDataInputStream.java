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
package org.apache.accumulo.core.file.streams;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.Seekable;

/**
 * A wrapper converting a {@link Seekable} {@code InputStream} into a {@code Seekable}
 * {@link DataInputStream}
 */
public class SeekableDataInputStream extends DataInputStream implements Seekable {
  public <StreamType extends InputStream & Seekable> SeekableDataInputStream(StreamType stream) {
    super(stream);
  }

  @Override
  public void seek(long pos) throws IOException {
    ((Seekable) in).seek(pos);
  }

  @Override
  public long getPos() throws IOException {
    return ((Seekable) in).getPos();
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return ((Seekable) in).seekToNewSource(targetPos);
  }
}
