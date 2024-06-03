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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

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
public interface FateStore<T> extends ReadOnlyFateStore<T> {

  /**
   * Create a new fate transaction id
   *
   * @return a new FateId
   */
  FateId create();

  /**
   * Creates and reserves a transaction using the given key. If something is already running for the
   * given key, then Optional.empty() will be returned. When this returns a non-empty id, it will be
   * in the new state.
   *
   * <p>
   * In the case where a process dies in the middle of a call to this. If later, another call is
   * made with the same key and its in the new state then the FateId for that key will be returned.
   * </p>
   *
   * @throws IllegalStateException when there is an unexpected collision. This can occur if two key
   *         hash to the same FateId or if a random FateId already exists.
   */
  Optional<FateTxStore<T>> createAndReserve(FateKey fateKey);

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
     * Return the given transaction to the store.
     *
     * upon successful return the store now controls the referenced transaction id. caller should no
     * longer interact with it.
     *
     * @param deferTime time in millis to keep this transaction from being returned by
     *        {@link #runnable(java.util.concurrent.atomic.AtomicBoolean, java.util.function.Consumer)}.
     *        Must be non-negative.
     */
    void unreserve(long deferTime, TimeUnit timeUnit);
  }

  /**
   * The value stored to indicate a FATE transaction ID ({@link FateId}) has been reserved
   */
  class FateReservation {

    // The LockID (provided by the Manager running the FATE which uses this store) which is used for
    // identifying dead Managers, so their reservations can be deleted and picked up again since
    // they can no longer be worked on.
    private final ZooUtil.LockID lockID; // TODO 4131 not sure if this is the best type for this
    // The UUID generated on a reservation attempt (tryReserve()) used to uniquely identify that
    // attempt. This is useful for the edge case where the reservation is sent to the server
    // (Tablet Server for UserFateStore and the ZooKeeper Server for MetaFateStore), but the server
    // dies before the store receives the response. It allows us to determine if the reservation
    // was successful and was written by this reservation attempt (could have been successfully
    // reserved by another attempt or not reserved at all, in which case, we wouldn't want to
    // expose a FateTxStore).
    private final UUID reservationUUID;
    private final byte[] serialized;
    private static final Pattern UUID_PATTERN =
        Pattern.compile("^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$");
    private static final Pattern LOCKID_PATTERN = Pattern.compile("^.+/.+\\$[0-9a-fA-F]+$");

    private FateReservation(ZooUtil.LockID lockID, UUID reservationUUID) {
      this.lockID = Objects.requireNonNull(lockID);
      this.reservationUUID = Objects.requireNonNull(reservationUUID);
      this.serialized = serialize(lockID, reservationUUID);
    }

    public static FateReservation from(ZooUtil.LockID lockID, UUID reservationUUID) {
      return new FateReservation(lockID, reservationUUID);
    }

    public static FateReservation from(byte[] serialized) {
      try (DataInputBuffer buffer = new DataInputBuffer()) {
        buffer.reset(serialized, serialized.length);
        ZooUtil.LockID lockID = new ZooUtil.LockID("", buffer.readUTF());
        UUID reservationUUID = UUID.fromString(buffer.readUTF());
        return new FateReservation(lockID, reservationUUID);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    public static FateReservation from(String fateReservationStr) {
      if (isFateReservation(fateReservationStr)) {
        String[] fields = fateReservationStr.split(":");
        ZooUtil.LockID lockId = new ZooUtil.LockID("", fields[0]);
        UUID reservationUUID = UUID.fromString(fields[1]);
        return new FateReservation(lockId, reservationUUID);
      } else {
        throw new IllegalArgumentException(
            "Tried to create a FateReservation from an invalid string: " + fateReservationStr);
      }
    }

    /**
     *
     * @param fateReservationStr the string from a call to FateReservations toString()
     * @return true if the string represents a valid FateReservation object, false otherwise
     */
    public static boolean isFateReservation(String fateReservationStr) {
      if (fateReservationStr != null) {
        String[] fields = fateReservationStr.split(":");
        if (fields.length == 2) {
          return LOCKID_PATTERN.matcher(fields[0]).matches()
              && UUID_PATTERN.matcher(fields[1]).matches();
        }
      }
      return false;
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
        dos.writeUTF(lockID.serialize("/"));
        dos.writeUTF(reservationUUID.toString());
        dos.close();
        return baos.toByteArray();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    public static FateReservation deserialize(byte[] serialized) {
      return FateReservation.from(serialized);
    }

    public static boolean locksAreEqual(ZooUtil.LockID lockID1, ZooUtil.LockID lockID2) {
      return lockID1.serialize("/").equals(lockID2.serialize("/"));
    }

    @Override
    public String toString() {
      return lockID.serialize("/") + ":" + reservationUUID;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof FateReservation) {
        FateReservation other = (FateReservation) obj;
        return this.lockID.serialize("/").equals(other.lockID.serialize("/"))
            && this.reservationUUID.equals(other.reservationUUID);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(lockID, reservationUUID);
    }
  }

  /**
   * @param fateId the fateId to check
   * @return true if the given fate id is reserved, false otherwise
   */
  boolean isReserved(FateId fateId);

  /**
   * @return a map of the current active reservations with the keys being the transaction that is
   *         reserved and the value being the value stored to indicate the transaction is reserved.
   */
  Map<FateId,FateReservation> getActiveReservations();

  /**
   * Deletes the current reservations which were reserved by a now dead Manager. These reservations
   * can no longer be worked on so their reservation should be deleted, so they can be picked up and
   * worked on again.
   */
  void deleteDeadReservations();

  /**
   * The way dead reservations are determined for {@link #deleteDeadReservations()}
   *
   * @param reservation the fate reservation
   * @return true if reservation held by a dead Manager, false otherwise
   */
  boolean isDeadReservation(FateReservation reservation);

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

}
