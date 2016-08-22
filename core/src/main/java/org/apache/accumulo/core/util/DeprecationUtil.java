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
package org.apache.accumulo.core.util;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.mapreduce.RangeInputSplit;

/**
 * A utility class for managing deprecated items. This avoids scattering private helper methods all over the code with warnings suppression.
 *
 * <p>
 * This class will never be public API and methods will be removed as soon as they are no longer needed. No methods in this class will, themselves, be
 * deprecated, because that would propagate the deprecation warning we are trying to avoid.
 *
 * <p>
 * This class should not be used as a substitute for deprecated classes. It should <b>only</b> be used for implementation code which must remain to support the
 * deprecated features, and <b>only</b> until that feature is removed.
 */
public class DeprecationUtil {

  @SuppressWarnings("deprecation")
  public static boolean isMockInstance(Instance instance) {
    return instance instanceof org.apache.accumulo.core.client.mock.MockInstance;
  }

  @SuppressWarnings("deprecation")
  public static Instance makeMockInstance(String instance) {
    return new org.apache.accumulo.core.client.mock.MockInstance(instance);
  }

  @SuppressWarnings("deprecation")
  public static void setMockInstance(RangeInputSplit split, boolean isMockInstance) {
    split.setMockInstance(isMockInstance);
  }

  @SuppressWarnings("deprecation")
  public static boolean isMockInstanceSet(RangeInputSplit split) {
    return split.isMockInstance();
  }

  @SuppressWarnings("deprecation")
  public static TabletLocator makeMockLocator() {
    return new org.apache.accumulo.core.client.mock.impl.MockTabletLocator();
  }

}
