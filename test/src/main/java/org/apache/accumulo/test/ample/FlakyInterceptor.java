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

import static org.apache.accumulo.core.client.ConditionalWriter.Status.UNKNOWN;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.test.ample.metadata.ConditionalWriterInterceptor;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlakyInterceptor implements ConditionalWriterInterceptor {

  private static final Logger log = LoggerFactory.getLogger(FlakyInterceptor.class);

  @Override
  public Iterator<ConditionalWriter.Result> write(ConditionalWriter writer,
      Iterator<ConditionalMutation> mutations) {
    ArrayList<ConditionalWriter.Result> results = new ArrayList<>();
    ArrayList<ConditionalMutation> mutationsToWrite = new ArrayList<>();

    // This code will randomly select from the following three options with equal probability for
    // each mutation.
    // 1. Do not write mutation and return UNKNOWN status
    // 2. Write muation and return UNKNOWN status
    // 3. Write mutation and return its actual status

    while (mutations.hasNext()) {
      var mutation = mutations.next();
      boolean dropMutation = RANDOM.get().nextDouble() <= .33;
      if (dropMutation) {
        // do not actually write the mutation and just return UNKNOWN
        results.add(new ConditionalWriter.Result(UNKNOWN, mutation, "flaky"));
        log.debug("Returning unknown for unwritten mutation with row: {}",
            new Text(mutation.getRow()));
      } else {
        // write this mutation and decide what to return for its status later
        mutationsToWrite.add(mutation);
      }
    }

    if (!mutationsToWrite.isEmpty()) {
      var realResults = writer.write(mutationsToWrite.iterator());
      while (realResults.hasNext()) {
        var result = realResults.next();
        // There is a 66% chance of arriving here for a given mutation. If the following two
        // branches each have a 50% chance here, then overall each branch has a 50% * 66% = 33%
        // chance. Therefore, all three possible terminal branches for a mutation have a 33% chance.
        boolean returnUnknown = RANDOM.get().nextBoolean();
        if (returnUnknown) {
          // the mutation was actually written, but return a result of unknown
          results.add(new ConditionalWriter.Result(UNKNOWN, result.getMutation(),
              result.getTabletServer()));
          log.debug("Returning unknown for written mutation with row: {}",
              new Text(result.getMutation().getRow()));
        } else {
          // return the actual status of the written mutation
          results.add(result);
        }
      }
    }

    return results.iterator();
  }

  @Override
  public ConditionalWriter.Result write(ConditionalWriter writer, ConditionalMutation mutation) {
    return write(writer, List.of(mutation).iterator()).next();
  }

}
