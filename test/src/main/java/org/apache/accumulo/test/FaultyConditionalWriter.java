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
package org.apache.accumulo.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.data.ConditionalMutation;

/**
 * A writer that will sometimes return unknown. When it returns unknown the condition may or may not have been written.
 */
public class FaultyConditionalWriter implements ConditionalWriter {

  private ConditionalWriter cw;
  private double up;
  private Random rand;
  private double wp;

  public FaultyConditionalWriter(ConditionalWriter cw, double unknownProbability, double writeProbability) {
    this.cw = cw;
    this.up = unknownProbability;
    this.wp = writeProbability;
    this.rand = new Random();

  }

  public Iterator<Result> write(Iterator<ConditionalMutation> mutations) {
    ArrayList<Result> resultList = new ArrayList<Result>();
    ArrayList<ConditionalMutation> writes = new ArrayList<ConditionalMutation>();

    while (mutations.hasNext()) {
      ConditionalMutation cm = mutations.next();
      if (rand.nextDouble() <= up && rand.nextDouble() > wp)
        resultList.add(new Result(Status.UNKNOWN, cm, null));
      else
        writes.add(cm);
    }

    if (writes.size() > 0) {
      Iterator<Result> results = cw.write(writes.iterator());

      while (results.hasNext()) {
        Result result = results.next();

        if (rand.nextDouble() <= up && rand.nextDouble() <= wp)
          result = new Result(Status.UNKNOWN, result.getMutation(), result.getTabletServer());
        resultList.add(result);
      }
    }
    return resultList.iterator();
  }

  public Result write(ConditionalMutation mutation) {
    return write(Collections.singleton(mutation).iterator()).next();
  }

  @Override
  public void close() {
    cw.close();
  }

}
