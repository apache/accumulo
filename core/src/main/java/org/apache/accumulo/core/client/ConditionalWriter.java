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
package org.apache.accumulo.core.client;

import java.util.Iterator;

import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.data.ConditionalMutation;

/**
 * ConditionalWriter provides the ability to do efficient, atomic read-modify-write operations on
 * rows. These operations are performed on the tablet server while a row lock is held.
 *
 * @since 1.6.0
 */
public interface ConditionalWriter extends AutoCloseable {
  class Result {

    private Status status;
    private ConditionalMutation mutation;
    private String server;
    private Exception exception;

    public Result(Status s, ConditionalMutation m, String server) {
      this.status = s;
      this.mutation = m;
      this.server = server;
    }

    public Result(Exception e, ConditionalMutation cm, String server) {
      this.exception = e;
      this.mutation = cm;
      this.server = server;
    }

    /**
     * If this method throws an exception, then its possible the mutation is still being actively
     * processed. Therefore if code chooses to continue after seeing an exception it should take
     * this into consideration.
     *
     * @return status of a conditional mutation
     */

    public Status getStatus() throws AccumuloException, AccumuloSecurityException {
      if (status == null) {
        if (exception instanceof AccumuloException) {
          throw new AccumuloException(exception);
        }
        if (exception instanceof AccumuloSecurityException) {
          AccumuloSecurityException ase = (AccumuloSecurityException) exception;
          throw new AccumuloSecurityException(ase.getUser(),
              SecurityErrorCode.valueOf(ase.getSecurityErrorCode().name()), ase.getTableInfo(),
              ase);
        } else {
          throw new AccumuloException(exception);
        }
      }

      return status;
    }

    /**
     *
     * @return A copy of the mutation previously submitted by a user. The mutation will reference
     *         the same data, but the object may be different.
     */
    public ConditionalMutation getMutation() {
      return mutation;
    }

    /**
     *
     * @return The server this mutation was sent to. Returns null if was not sent to a server.
     */
    public String getTabletServer() {
      return server;
    }
  }

  enum Status {
    /**
     * conditions were met and mutation was written
     */
    ACCEPTED,
    /**
     * conditions were not met and mutation was not written
     */
    REJECTED,
    /**
     * mutation violated a constraint and was not written
     */
    VIOLATED,
    /**
     * error occurred after mutation was sent to server, its unknown if the mutation was written.
     * Although the status of the mutation is unknown, Accumulo guarantees the mutation will not be
     * written at a later point in time.
     */
    UNKNOWN,
    /**
     * A condition contained a column visibility that could never be seen
     */
    INVISIBLE_VISIBILITY,

  }

  /**
   * This method returns one result for each mutation passed to it. This method is thread safe.
   * Multiple threads can safely use a single conditional writer. Sharing a conditional writer
   * between multiple threads may result in batching of request to tablet servers.
   *
   * @return Result for each mutation submitted. The mutations may still be processing in the
   *         background when this method returns, if so the iterator will block.
   */
  Iterator<Result> write(Iterator<ConditionalMutation> mutations);

  /**
   * This method has the same thread safety guarantees as {@link #write(Iterator)}
   *
   * @return Result for the submitted mutation
   */

  Result write(ConditionalMutation mutation);

  /**
   * release any resources (like threads pools) used by conditional writer
   */
  @Override
  void close();
}
