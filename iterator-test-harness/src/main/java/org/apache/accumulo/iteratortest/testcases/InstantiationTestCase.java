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
package org.apache.accumulo.iteratortest.testcases;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.iteratortest.IteratorTestCase;
import org.apache.accumulo.iteratortest.IteratorTestInput;
import org.apache.accumulo.iteratortest.IteratorTestOutput;
import org.apache.accumulo.iteratortest.IteratorTestOutput.TestOutcome;

/**
 * TestCase to assert that an Iterator has a no-args constructor.
 */
public class InstantiationTestCase implements IteratorTestCase {

  @Override
  public IteratorTestOutput test(IteratorTestInput testInput) {
    Class<? extends SortedKeyValueIterator<Key,Value>> clz = testInput.getIteratorClass();

    try {
      // We should be able to instantiate the Iterator given the Class
      clz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      return new IteratorTestOutput(e);
    }

    return new IteratorTestOutput(TestOutcome.PASSED);
  }

  @Override
  public boolean verify(IteratorTestOutput expected, IteratorTestOutput actual) {
    // Ignore what the user provided as expected output, just check that we instantiated the
    // iterator successfully.
    return actual.getTestOutcome() == TestOutcome.PASSED;
  }

}
