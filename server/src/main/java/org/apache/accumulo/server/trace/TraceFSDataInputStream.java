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
package org.apache.accumulo.server.trace;

import java.io.IOException;

import org.apache.accumulo.trace.instrument.Span;
import org.apache.accumulo.trace.instrument.Trace;
import org.apache.hadoop.fs.FSDataInputStream;

public class TraceFSDataInputStream extends FSDataInputStream {
  @Override
  public synchronized void seek(long desired) throws IOException {
    Span span = Trace.start("FSDataInputStream.seek");
    try {
      impl.seek(desired);
    } finally {
      span.stop();
    }
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    Span span = Trace.start("FSDataInputStream.read");
    if (Trace.isTracing())
      span.data("length", Integer.toString(length));
    try {
      return impl.read(position, buffer, offset, length);
    } finally {
      span.stop();
    }
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    Span span = Trace.start("FSDataInputStream.readFully");
    if (Trace.isTracing())
      span.data("length", Integer.toString(length));
    try {
      impl.readFully(position, buffer, offset, length);
    } finally {
      span.stop();
    }
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    Span span = Trace.start("FSDataInputStream.readFully");
    if (Trace.isTracing())
      span.data("length", Integer.toString(buffer.length));
    try {
      impl.readFully(position, buffer);
    } finally {
      span.stop();
    }
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    Span span = Trace.start("FSDataInputStream.seekToNewSource");
    try {
      return impl.seekToNewSource(targetPos);
    } finally {
      span.stop();
    }
  }

  private final FSDataInputStream impl;

  public TraceFSDataInputStream(FSDataInputStream in) throws IOException {
    super(in);
    impl = in;
  }

}
