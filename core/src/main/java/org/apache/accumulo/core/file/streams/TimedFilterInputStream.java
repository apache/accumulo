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

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.hadoop.fs.Seekable;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

public class TimedFilterInputStream extends FilterInputStream implements Seekable {

  interface IoeSupplierInt {
    Integer get() throws IOException;
  }

  interface IoeSupplierVoid {
    void get() throws IOException;
  }

  interface IoeSupplierLong {
    Long get() throws IOException;
  }

  interface IoeSupplierBool {
    Boolean get() throws IOException;
  }

  private final Timer seekTimer;
  private final Timer readTimer;
  private final org.apache.accumulo.core.util.Timer methodTimer =
      org.apache.accumulo.core.util.Timer.startNew();

  public <StreamType extends InputStream & Seekable> TimedFilterInputStream(StreamType in) {
    super(in);
    seekTimer = Timer.builder(MetricsProducer.BCFILE_SEEK_OPS).description("")
        .register(Metrics.globalRegistry);
    readTimer = Timer.builder(MetricsProducer.BCFILE_READ_OPS).description("")
        .register(Metrics.globalRegistry);
  }

  private int timeOperation(IoeSupplierInt op) throws IOException {
    try {
      methodTimer.restart();
      return op.get();
    } finally {
      readTimer.record(methodTimer.elapsed());
    }
  }

  @Override
  public int read() throws IOException {
    return timeOperation(() -> super.read());
  }

  @Override
  public int read(byte[] b) throws IOException {
    return timeOperation(() -> super.read(b));
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return timeOperation(() -> super.read(b, off, len));
  }

  private void timeOperation(IoeSupplierVoid op) throws IOException {
    try {
      methodTimer.restart();
      op.get();
    } finally {
      seekTimer.record(methodTimer.elapsed());
    }
  }

  private long timeOperation(IoeSupplierLong op) throws IOException {
    try {
      methodTimer.restart();
      return op.get();
    } finally {
      seekTimer.record(methodTimer.elapsed());
    }
  }

  private boolean timeOperation(IoeSupplierBool op) throws IOException {
    try {
      methodTimer.restart();
      return op.get();
    } finally {
      seekTimer.record(methodTimer.elapsed());
    }
  }

  @Override
  public void seek(long pos) throws IOException {
    timeOperation(() -> ((Seekable) in).seek(pos));
  }

  @Override
  public long getPos() throws IOException {
    return timeOperation(() -> ((Seekable) in).getPos());
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return timeOperation(() -> ((Seekable) in).seekToNewSource(targetPos));
  }

}
