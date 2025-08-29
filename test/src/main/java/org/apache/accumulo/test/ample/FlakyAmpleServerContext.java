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
package org.apache.accumulo.test.ample;

import java.util.Map;
import java.util.function.Supplier;

import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.ample.metadata.TestAmple;

import com.google.common.base.Suppliers;

/**
 * A goal of this class is to exercise the lambdas passed to
 * {@link org.apache.accumulo.core.metadata.schema.Ample.ConditionalTabletMutator#submit(Ample.RejectionHandler)}.
 * This done by returning a version of Ample that randomly returns UNKNOWN for conditional mutations
 * using the {@link FlakyInterceptor}.
 */
public class FlakyAmpleServerContext extends ServerContext {

  private final Supplier<Ample> ampleSupplier;

  public FlakyAmpleServerContext(SiteConfiguration siteConfig) {
    super(siteConfig);
    // Each instance of TestAmple created will create a new Hadoop configuration object. These
    // seemed to hang around and cause OOME and process death. Did not track down why they were
    // hanging around, but decided to avoid creating a new instance of TestAmple each time Ample is
    // requested in order to avoid creating those hadoop config objects.
    ampleSupplier = Suppliers.memoize(() -> TestAmple.create(
        this, Map.of(Ample.DataLevel.USER, Ample.DataLevel.USER.metaTable(),
            Ample.DataLevel.METADATA, Ample.DataLevel.METADATA.metaTable()),
        FlakyInterceptor::new));
  }

  @Override
  public Ample getAmple() {
    return ampleSupplier.get();
  }
}
