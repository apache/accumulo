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
package org.apache.accumulo.server.metadata;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.List;
import java.util.Objects;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.constraints.Constraint;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMutatorBase;
import org.apache.accumulo.core.security.AuthorizationContainer;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.constraints.MetadataConstraints;
import org.apache.accumulo.server.constraints.SystemEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RootTabletMutatorImpl extends TabletMutatorBase<Ample.TabletMutator>
    implements Ample.TabletMutator {

  private final ServiceLock lock;
  private final ServerContext context;

  private static final Logger log = LoggerFactory.getLogger(RootTabletMutatorImpl.class);

  static class RootEnv implements SystemEnvironment, Constraint.Environment {

    private final ServerContext context;

    RootEnv(ServerContext context) {
      this.context = context;
    }

    @Override
    public TabletId getTablet() {
      return new TabletIdImpl(RootTable.EXTENT);
    }

    @Override
    public String getUser() {
      throw new UnsupportedOperationException();
    }

    @Override
    public AuthorizationContainer getAuthorizationsContainer() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ServerContext getServerContext() {
      return context;
    }
  }

  RootTabletMutatorImpl(ServerContext context) {
    super(RootTable.EXTENT);
    this.context = context;
    this.lock = this.context.getServiceLock();
    Objects.requireNonNull(this.lock, "ServiceLock not set on ServerContext");
  }

  @Override
  public void mutate() {

    this.putZooLock(this.context.getZooKeeperRoot(), lock);
    Mutation mutation = getMutation();

    MetadataConstraints metaConstraint = new MetadataConstraints();
    List<Short> violations = metaConstraint.check(new RootEnv(context), mutation);

    if (violations != null && !violations.isEmpty()) {
      throw new IllegalStateException(
          "Mutation for root tablet metadata violated constraints : " + violations);
    }

    try {
      String zpath = context.getZooKeeperRoot() + RootTable.ZROOT_TABLET;

      context.getZooCache().clear(zpath);

      // TODO examine implementation of getZooReaderWriter().mutate()
      // TODO for efficiency this should maybe call mutateExisting
      context.getZooReaderWriter().mutateOrCreate(zpath, new byte[0], currVal -> {
        String currJson = new String(currVal, UTF_8);
        var rtm = new RootTabletMetadata(currJson);
        rtm.update(mutation);
        String newJson = rtm.toJson();
        log.debug("mutation: from:[{}] to: [{}]", currJson, newJson);
        return newJson.getBytes(UTF_8);
      });

      // TODO this is racy...
      context.getZooCache().clear(zpath);

      if (closeAfterMutate != null) {
        closeAfterMutate.close();
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
