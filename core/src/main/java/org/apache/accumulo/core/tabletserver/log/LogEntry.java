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
package org.apache.accumulo.core.tabletserver.log;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Base64;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.spi.wal.WalProvider;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

public final class LogEntry {

  private static final String LOG_PATH =
      "%s" + Path.SEPARATOR + Constants.WAL_DIR + Path.SEPARATOR + "%s";
  private static final String ADDRESS_QUALIFIER_PREFIX = "@";
  private static final String METADATA_VERSION = "v2";

  private final String reference;
  private final @Nullable Path legacyPath;
  private final String providerId;
  private final @Nullable HostAndPort tserverAddress;
  private final UUID uniqueId;
  private final Text columnQualifier;

  private LogEntry(String reference, @Nullable Path legacyPath, String providerId,
      @Nullable HostAndPort tserverAddress, UUID uniqueId, Text columnQualifier) {
    this.reference = reference;
    this.legacyPath = legacyPath;
    this.providerId = providerId;
    this.tserverAddress = tserverAddress;
    this.uniqueId = uniqueId;
    this.columnQualifier = columnQualifier;
  }

  public static LogEntry withSwitchedVolume(LogEntry entry, Path newPath) {
    if (entry.legacyPath != null) {
      Path switchedPath = validatePath(newPath);
      return new LogEntry(switchedPath.toString(), switchedPath, entry.providerId,
          entry.getTServerAddress(), entry.uniqueId, entry.columnQualifier);
    }
    HostAndPort address = entry.getTServerAddress();
    return address == null ? fromReference(entry.providerId, newPath.toString(), entry.uniqueId)
        : fromReference(entry.providerId, newPath.toString(), entry.uniqueId, address);
  }

  public static LogEntry forServer(String providerId, String volume, HostAndPort address) {
    UUID id = UUID.randomUUID();
    String filePath = LOG_PATH.formatted(volume, id);
    return fromPath(providerId, filePath, address);
  }

  /**
   * Creates a log entry for a provider-owned reference. The reference is opaque to Accumulo, but
   * must contain the UUID supplied to {@link org.apache.accumulo.core.spi.wal.WalProvider#create}.
   */
  public static LogEntry fromReference(String providerId, String reference, UUID uniqueId,
      HostAndPort tserverAddress) {
    return validatedReference(reference, providerId, validateTserverAddress(tserverAddress),
        uniqueId, null);
  }

  /** Creates a log entry for a provider-owned reference when the creator address is not known. */
  public static LogEntry fromReference(String providerId, String reference, UUID uniqueId) {
    return validatedReference(reference, providerId, null, uniqueId, null);
  }

  /**
   * Creates a legacy path-based LogEntry. The path must be an absolute filesystem URI whose final
   * component is a UUID. When possible, the TabletServer address is inferred from the penultimate
   * component of legacy WAL paths.
   *
   * @param providerId provider that owns this WAL
   * @param path path to validate
   * @return an object representation of this log entry
   * @throws IllegalArgumentException if the path is invalid
   */
  public static LogEntry fromPath(String providerId, String path) {
    return validatedLogEntry(path, providerId, getLegacyTserverAddress(path), null);
  }

  /**
   * Creates a LogEntry using an explicitly known TabletServer address.
   *
   * @param providerId provider that owns this WAL
   * @param path legacy fully qualified path-based WAL reference
   * @param tserverAddress TabletServer address that created this WAL
   */
  public static LogEntry fromPath(String providerId, String path, HostAndPort tserverAddress) {
    return validatedLogEntry(path, providerId, validateTserverAddress(tserverAddress), null);
  }

  private static LogEntry validatedLogEntry(String path, String providerId,
      @Nullable HostAndPort tserverAddress, Text columnQualifier) {
    validateProviderId(providerId);
    Path logPath = validatePath(new Path(path));
    String uuidPart = logPath.getName();
    String badUuidMsg = "Expected valid UUID as the final path component. Found '" + uuidPart + "'";
    UUID uuid;
    try {
      uuid = UUID.fromString(uuidPart);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(badUuidMsg);
    }
    if (!uuid.toString().equals(uuidPart)) {
      throw new IllegalArgumentException(badUuidMsg);
    }
    return new LogEntry(logPath.toString(), logPath, providerId, tserverAddress, uuid,
        columnQualifier);
  }

  private static LogEntry validatedReference(String reference, String providerId,
      @Nullable HostAndPort tserverAddress, UUID uniqueId, Text columnQualifier) {
    validateProviderId(providerId);
    Preconditions.checkArgument(reference != null && !reference.isBlank(),
        "WAL reference must not be blank");
    Objects.requireNonNull(uniqueId, "WAL UUID must not be null");
    Preconditions.checkArgument(reference.contains(uniqueId.toString()),
        "WAL reference must contain the requested UUID %s", uniqueId);
    return new LogEntry(reference, null, providerId, tserverAddress, uniqueId, columnQualifier);
  }

  private static void validateProviderId(String providerId) {
    Preconditions.checkArgument(
        providerId != null && providerId.matches(WalProvider.PROVIDER_ID_REGEX),
        "Invalid WAL provider ID: %s", providerId);
  }

  private static HostAndPort validateTserverAddress(HostAndPort address) {
    Preconditions.checkArgument(address != null && address.hasPort(),
        "TabletServer address must include a port");
    return address;
  }

  @Nullable
  private static HostAndPort getLegacyTserverAddress(String path) {
    Path parent = new Path(path).getParent();
    if (parent == null) {
      return null;
    }
    String address = parent.getName();
    if (!address.contains("+")) {
      return null;
    }
    try {
      return validateTserverAddress(HostAndPort.fromString(address.replace('+', ':')));
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static Path validatePath(Path path) {
    if (path.toUri().getScheme() == null || !path.isAbsolute()) {
      throw new IllegalArgumentException(
          "Invalid path format. The path must be an absolute URI: " + path);
    }
    return path;
  }

  /**
   * Construct a new LogEntry object after deserializing it from a metadata entry.
   *
   * @param entry the metadata entry
   * @return a new LogEntry object constructed from the path stored in the column qualifier
   * @throws IllegalArgumentException if the path stored in the metadata entry is invalid or if the
   *         serialized format of the entry is unrecognized
   */
  public static LogEntry fromMetaWalEntry(Entry<Key,Value> entry) {
    Text fam = entry.getKey().getColumnFamily();
    Preconditions.checkArgument(LogColumnFamily.NAME.equals(fam),
        "The provided metadata entry's column family is %s instead of %s", fam,
        LogColumnFamily.NAME);
    Text qualifier = entry.getKey().getColumnQualifier();
    String qualifierString = qualifier.toString();
    int providerSeparator = qualifierString.indexOf('/');
    Preconditions.checkArgument(providerSeparator > 0, "Malformed write-ahead log %s", qualifier);
    String providerId = qualifierString.substring(0, providerSeparator);
    String rest = qualifierString.substring(providerSeparator + 1);
    Preconditions.checkArgument(rest.startsWith(ADDRESS_QUALIFIER_PREFIX),
        "WAL qualifier does not contain a TabletServer address: %s", qualifier);
    int addressSeparator = rest.indexOf('/', ADDRESS_QUALIFIER_PREFIX.length());
    Preconditions.checkArgument(addressSeparator > ADDRESS_QUALIFIER_PREFIX.length(),
        "Malformed WAL TabletServer address in qualifier %s", qualifier);
    HostAndPort address;
    try {
      address = validateTserverAddress(HostAndPort
          .fromString(rest.substring(ADDRESS_QUALIFIER_PREFIX.length(), addressSeparator)));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Malformed WAL TabletServer address in qualifier " + qualifier, e);
    }
    String encodedReference = rest.substring(addressSeparator + 1);
    if (encodedReference.startsWith(METADATA_VERSION + "/")) {
      String[] fields = encodedReference.split("/", 4);
      Preconditions.checkArgument(fields.length == 4, "Malformed versioned WAL qualifier: %s",
          qualifier);
      UUID uuid;
      try {
        uuid = UUID.fromString(fields[2]);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Malformed WAL UUID in qualifier " + qualifier, e);
      }
      Preconditions.checkArgument(uuid.toString().equals(fields[2]),
          "Malformed WAL UUID in qualifier %s", qualifier);
      String reference;
      try {
        reference = new String(Base64.getUrlDecoder().decode(fields[3]), UTF_8);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Malformed WAL reference in qualifier " + qualifier, e);
      }
      if (fields[1].equals("legacy")) {
        LogEntry legacy = validatedLogEntry(reference, providerId, address, qualifier);
        Preconditions.checkArgument(legacy.getUniqueID().equals(uuid),
            "WAL UUID in qualifier does not match legacy reference: %s", qualifier);
        return legacy;
      }
      Preconditions.checkArgument(fields[1].equals("opaque"),
          "Unknown WAL qualifier reference type %s", fields[1]);
      return validatedReference(reference, providerId, address, uuid, qualifier);
    }
    return validatedLogEntry(encodedReference, providerId, address, qualifier);
  }

  @Nullable
  public HostAndPort getTServerAddress() {
    return tserverAddress;
  }

  @NonNull
  public Path getPath() {
    if (legacyPath == null) {
      throw new IllegalStateException(
          "WAL reference is opaque and is not necessarily a Hadoop path");
    }
    return legacyPath;
  }

  /** Returns the raw provider-owned WAL reference. */
  public String getReference() {
    return reference;
  }

  /** Returns whether this entry was read from the legacy path-based reference format. */
  public boolean isLegacyPathReference() {
    return legacyPath != null;
  }

  public String getProviderId() {
    return providerId;
  }

  @NonNull
  public UUID getUniqueID() {
    return uniqueId;
  }

  @Override
  public String toString() {
    return reference;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other instanceof LogEntry) {
      LogEntry that = (LogEntry) other;
      return reference.equals(that.reference) && providerId.equals(that.providerId)
          && uniqueId.equals(that.uniqueId);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(providerId, reference, uniqueId);
  }

  /**
   * Get the Text that should be used as the column qualifier to store this as a metadata entry.
   */
  public Text getColumnQualifier() {
    return columnQualifier == null ? newCQ() : new Text(columnQualifier);
  }

  private Text newCQ() {
    HostAndPort address = Objects.requireNonNull(tserverAddress,
        "TabletServer address is required to write a WAL metadata qualifier");
    String encodedReference =
        Base64.getUrlEncoder().withoutPadding().encodeToString(reference.getBytes(UTF_8));
    String referenceType = legacyPath == null ? "opaque" : "legacy";
    return new Text(providerId + "/" + ADDRESS_QUALIFIER_PREFIX + address + "/" + METADATA_VERSION
        + "/" + referenceType + "/" + uniqueId + "/" + encodedReference);
  }

  /**
   * Put a delete marker in the provided mutation for this LogEntry.
   *
   * @param mutation the mutation to update
   */
  public void deleteFromMutation(Mutation mutation) {
    mutation.putDelete(LogColumnFamily.NAME, getColumnQualifier());
  }

  /**
   * Put this LogEntry into the provided mutation.
   *
   * @param mutation the mutation to update
   */
  public void addToMutation(Mutation mutation) {
    mutation.put(LogColumnFamily.NAME, newCQ(), new Value());
  }

}
