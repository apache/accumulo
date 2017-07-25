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
package org.apache.accumulo.core.file.streams;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

import org.apache.hadoop.fs.FSDataOutputStream;

/**
 * Utility functions for {@link PositionedOutput}.
 */
public class PositionedOutputs {
  private PositionedOutputs() {}

  /** Convert an {@code OutputStream} into an {@code OutputStream} implementing {@link PositionedOutput}. */
  public static PositionedOutputStream wrap(final OutputStream fout) {
    Objects.requireNonNull(fout);
    if (fout instanceof FSDataOutputStream) {
      return new PositionedOutputStream(fout) {
        @Override
        public long position() throws IOException {
          return ((FSDataOutputStream) fout).getPos();
        }
      };
    } else if (fout instanceof PositionedOutput) {
      return new PositionedOutputStream(fout) {
        @Override
        public long position() throws IOException {
          return ((PositionedOutput) fout).position();
        }
      };
    } else {
      return new PositionedOutputStream(fout) {
        @Override
        public long position() throws IOException {
          throw new UnsupportedOperationException("Underlying stream does not support position()");
        }
      };
    }
  }

  private static abstract class PositionedOutputStream extends FilterOutputStream implements PositionedOutput {
    public PositionedOutputStream(OutputStream stream) {
      super(stream);
    }

    @Override
    public void write(byte[] data, int off, int len) throws IOException {
      out.write(data, off, len);
    }
  }
}
