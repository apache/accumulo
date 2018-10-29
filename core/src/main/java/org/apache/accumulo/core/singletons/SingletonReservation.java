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

package org.apache.accumulo.core.singletons;

/**
 * @see SingletonManager#getClientReservation()
 */
public class SingletonReservation implements AutoCloseable {

  private boolean closed = false;

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    SingletonManager.releaseRerservation();
  }

  @Override
  protected void finalize() throws Throwable {
    try {
      close();
    } finally {
      super.finalize();
    }
  }

  private static final SingletonReservation FAKE = new SingletonReservation() {
    @Override
    public void close() {}
  };

  /**
   * @return A fake reservation where the close method is a no-op.
   */
  public static SingletonReservation fake() {
    return FAKE;
  }
}
