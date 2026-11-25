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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.accumulo.core.logging.FileOperationsLogger;
import org.apache.accumulo.core.util.Timer;
import org.apache.hadoop.fs.Seekable;

public class LoggingFilterInputStream extends FilterInputStream implements Seekable {

  private final Timer methodTimer = Timer.startNew();

  public <StreamType extends InputStream & Seekable> LoggingFilterInputStream(StreamType in) {
    super(in);
  }

  @Override
  public int read() throws IOException {
    methodTimer.restart();
    int read = super.read();
    FileOperationsLogger.readCalled(read, methodTimer.elapsed());
    return read;
  }

  @Override
  public int read(byte[] b) throws IOException {
    methodTimer.restart();
    int read = super.read(b);
    FileOperationsLogger.readCalled(read, methodTimer.elapsed());
    return read;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    methodTimer.restart();
    int read = super.read(b, off, len);
    FileOperationsLogger.readCalled(read, methodTimer.elapsed());
    return read;
  }

  @Override
  public void seek(long pos) throws IOException {
    methodTimer.restart();
    ((Seekable) in).seek(pos);
    FileOperationsLogger.seekCalled(pos, methodTimer.elapsed());
  }

  @Override
  public long getPos() throws IOException {
    return ((Seekable) in).getPos();
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    methodTimer.restart();
    boolean newSource = ((Seekable) in).seekToNewSource(targetPos);
    FileOperationsLogger.seekCalled(targetPos, methodTimer.elapsed());
    return newSource;
  }

}
