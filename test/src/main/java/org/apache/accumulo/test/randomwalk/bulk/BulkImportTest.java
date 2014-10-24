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
package org.apache.accumulo.test.randomwalk.bulk;

import java.util.Properties;

import org.apache.accumulo.test.randomwalk.State;

/**
 * If we have a sufficient back-up of imports, let them work off before adding even more bulk-imports. Imports of PlusOne must always be balanced with imports
 * of MinusOne.
 */
public abstract class BulkImportTest extends BulkTest {

  public static final String SKIPPED_IMPORT = "skipped.import", TRUE = Boolean.TRUE.toString(), FALSE = Boolean.FALSE.toString();

  @Override
  public void visit(final State state, Properties props) throws Exception {
    /**
     * Each visit() is performed sequentially and then submitted to the threadpool which will have async execution. As long as we're checking the state and
     * making decisions about what to do before we submit something to the thread pool, we're fine.
     */

    String lastImportSkipped = state.getString(SKIPPED_IMPORT);
    // We have a marker in the state for the previous insert, we have to balance skipping BulkPlusOne
    // with skipping the new BulkMinusOne to make sure that we maintain consistency
    if (null != lastImportSkipped) {
      if (!getClass().equals(BulkMinusOne.class)) {
        throw new IllegalStateException("Should not have a skipped import marker for a class other than " + BulkMinusOne.class.getName() + " but was "
            + getClass().getName());
      }

      if (TRUE.equals(lastImportSkipped)) {
        log.debug("Last import was skipped, skipping this import to ensure consistency");
        state.remove(SKIPPED_IMPORT);

        // Wait 30s to balance the skip of a BulkPlusOne/BulkMinusOne pair
        log.debug("Waiting 30s before continuing");
        try {
          Thread.sleep(30 * 1000);
        } catch (InterruptedException e) {}

        return;
      } else {
        // last import was not skipped, remove the marker
        state.remove(SKIPPED_IMPORT);
      }
    }

    if (shouldQueueMoreImports(state)) {
      super.visit(state, props);
    } else {
      log.debug("Not queuing more imports this round because too many are already queued");
      state.set(SKIPPED_IMPORT, TRUE);
      // Don't sleep here, let the sleep happen when we skip the next BulkMinusOne
    }
  }

  private boolean shouldQueueMoreImports(State state) throws Exception {
    // Only selectively import when it's BulkPlusOne. If we did a BulkPlusOne,
    // we must also do a BulkMinusOne to keep the table consistent
    if (getClass().equals(BulkPlusOne.class)) {
      // Only queue up more imports if the number of queued tasks already
      // exceeds the number of tservers by 50x
      return SelectiveQueueing.shouldQueueOperation(state);
    }

    return true;
  }
}
