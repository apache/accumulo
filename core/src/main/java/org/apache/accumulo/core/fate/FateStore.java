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
package org.apache.accumulo.core.fate;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.hadoop.io.DataInputBuffer;

/**
 * Transaction Store: a place to save transactions
 *
 * A transaction consists of a number of operations. To use, first create a fate transaction id, and
 * then seed the transaction with an initial operation. An executor service can then execute the
 * transaction's operation, possibly pushing more operations onto the transaction as each step
 * successfully completes. If a step fails, the stack can be unwound, undoing each operation.
 */
public interface FateStore<T> extends ReadOnlyFateStore<T>, AutoCloseable {

  /**
   * Create a new fate transaction id
   *
   * @return a new FateId
   */
  FateId create();

  interface Seeder<T> extends AutoCloseable {

    /**
     * Attempts to seed a transaction with the given repo if it does not exist. A fateId will be
     * derived from the fateKey. If seeded, sets the following data for the fateId in the store.
     *
     * <ul>
     * <li>Set the fate op</li>
     * <li>Set the status to SUBMITTED</li>
     * <li>Set the fate key</li>
     * <li>Sets autocleanup only if true</li>
     * <li>Sets the creation time</li>
     * </ul>
     *
     * @return The return type is only intended for testing it may not be correct in the face of
     *         failures. When there are no failures returns optional w/ the fate id set if seeded
     *         and empty optional otherwise. If there was a failure this could return an empty
     *         optional when it actually succeeded.
     */
    CompletableFuture<Optional<FateId>> attemptToSeedTransaction(Fate.FateOperation fateOp,
        FateKey fateKey, Repo<T> repo, boolean autoCleanUp);

    @Override
    void close();
  }

  // Creates a conditional writer for the user fate store. For Zookeeper this will be a no-op
  // because currently zookeeper does not support multi-node operations.
  Seeder<T> beginSeeding();

  /**
   * Seeds a transaction with the given repo if its current status is NEW and it is currently
   * unreserved. If seeded, sets the following data for the fateId in the store.
   *
   * <ul>
   * <li>Set the fate op</li>
   * <li>Set the status to SUBMITTED</li>
   * <li>Sets autocleanup only if true</li>
   * <li>Sets the creation time</li>
   * </ul>
   *
   * @return The return type is only intended for testing it may not be correct in the face of
   *         failures. When there are no failures returns true if seeded and false otherwise. If
   *         there was a failure this could return false when it actually succeeded.
   */
  boolean seedTransaction(Fate.FateOperation fateOp, FateId fateId, Repo<T> repo,
      boolean autoCleanUp);

  /**
   * An interface that allows read/write access to the data related to a single fate operation.
   */
  interface FateTxStore<T> extends ReadOnlyFateTxStore<T> {
    @Override
    Repo<T> top();

    /**
     * Update the given transaction with the next operation
     *
     * @param repo the operation
     */
    void push(Repo<T> repo) throws StackOverflowException;

    /**
     * Remove the last pushed operation from the given transaction.
     */
    void pop();

    /**
     * Update the state of a given transaction
     *
     * @param status execution status
     */
    void setStatus(TStatus status);

    /**
     * Set transaction-specific information.
     *
     * @param txInfo name of attribute of a transaction to set.
     * @param val transaction data to store
     */
    void setTransactionInfo(Fate.TxInfo txInfo, Serializable val);

    /**
     * Remove the transaction from the store.
     *
     */
    void delete();

    /**
     * Force remove the transaction from the store regardless of the status. Only to be used by
     * {@link AdminUtil}
     */
    void forceDelete();

    /**
     * Return the given transaction to the store.
     *
     * upon successful return the store now controls the referenced transaction id. caller should no
     * longer interact with it.
     *
     * @param deferTime time to keep this transaction from being returned by
     *        {@link #runnable(BooleanSupplier, Consumer)}. Must be non-negative.
     */
    void unreserve(Duration deferTime);
  }

  /**
   * The value stored to indicate a FATE transaction ID ({@link FateId}) has been reserved
   */
  final class FateReservation {

    // The LockID (provided by the Manager running the FATE which uses this store) which is used for
    // identifying dead Managers, so their reservations can be deleted and picked up again since
    // they can no longer be worked on.
    private final ZooUtil.LockID lockID;
    // The UUID generated on a reservation attempt (tryReserve()) used to uniquely identify that
    // attempt. This is useful for the edge case where the reservation is sent to the server
    // (Tablet Server for UserFateStore and the ZooKeeper Server for MetaFateStore), but the server
    // dies before the store receives the response. It allows us to determine if the reservation
    // was successful and was written by this reservation attempt (could have been successfully
    // reserved by another attempt or not reserved at all, in which case, we wouldn't want to
    // expose a FateTxStore).
    private final UUID reservationUUID;
    private final byte[] serialized;

    private FateReservation(ZooUtil.LockID lockID, UUID reservationUUID) {
      this.lockID = Objects.requireNonNull(lockID);
      this.reservationUUID = Objects.requireNonNull(reservationUUID);
      this.serialized = serialize(lockID, reservationUUID);
    }

    public static FateReservation from(ZooUtil.LockID lockID, UUID reservationUUID) {
      return new FateReservation(lockID, reservationUUID);
    }

    public ZooUtil.LockID getLockID() {
      return lockID;
    }

    public UUID getReservationUUID() {
      return reservationUUID;
    }

    public byte[] getSerialized() {
      return serialized;
    }

    private static byte[] serialize(ZooUtil.LockID lockID, UUID reservationUUID) {
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
          DataOutputStream dos = new DataOutputStream(baos)) {
        dos.writeUTF(lockID.serialize());
        dos.writeUTF(reservationUUID.toString());
        dos.close();
        return baos.toByteArray();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    public static FateReservation deserialize(byte[] serialized) {
      try (DataInputBuffer buffer = new DataInputBuffer()) {
        buffer.reset(serialized, serialized.length);
        ZooUtil.LockID lockID = ZooUtil.LockID.deserialize(buffer.readUTF());
        UUID reservationUUID = UUID.fromString(buffer.readUTF());
        return new FateReservation(lockID, reservationUUID);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public String toString() {
      return lockID.serialize() + ":" + reservationUUID;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj instanceof FateReservation other) {
        return Arrays.equals(this.getSerialized(), other.getSerialized());
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(lockID, reservationUUID);
    }
  }

  /**
   * Deletes the current reservations which were reserved by a now dead Manager. These reservations
   * can no longer be worked on so their reservation should be deleted, so they can be picked up and
   * worked on again.
   */
  void deleteDeadReservations();

  /**
   * Attempt to reserve the fate transaction.
   *
   * @param fateId The FateId
   * @return An Optional containing the {@link FateTxStore} if the transaction was successfully
   *         reserved, or an empty Optional if the transaction was not able to be reserved.
   */
  Optional<FateTxStore<T>> tryReserve(FateId fateId);

  /**
   * Reserve the fate transaction.
   *
   * Reserving a fate transaction ensures that nothing else in-process interacting via the same
   * instance will be operating on that fate transaction.
   *
   */
  FateTxStore<T> reserve(FateId fateId);

  @Override
  void close();
}
