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
package org.apache.accumulo.start.spi;

import java.util.ServiceLoader;

/**
 * An interface used with the Java {@link ServiceLoader} to auto-discover classes executable with a convenient keyword on the command-line.
 *
 * <p>
 * All implementing classes who have an entry in META-INF/services/{@link org.apache.accumulo.start.spi.KeywordExecutable} on the classpath will be constructed
 * by the {@link ServiceLoader}, so they should be lightweight and quickly constructible with a mandatory no-argument constructor. Because of this, implementing
 * classes could simply be factories which execute a different class, if that class is expensive to construct or cannot have a no-argument constructor.
 *
 * <p>
 * One way to easily create META-INF/services files is to use the <a href="https://github.com/google/auto/tree/master/service">AutoService</a> annotation.
 *
 * <p>
 * If the implementing class also wishes to have a redundant main method, it may be useful to simply implement main as:<br>
 * {@code new MyImplementingClass().execute(args);}
 *
 */
public interface KeywordExecutable {

  public static enum UsageGroup {
    CORE, PROCESS, OTHER;
  }

  /**
   * @return Keyword which identifies this service
   */
  String keyword();

  /**
   * @return Usage for service
   */
  default String usage() {
    return keyword();
  }

  /**
   * @return Usage group for this command
   */
  default UsageGroup usageGroup() {
    return UsageGroup.OTHER;
  }

  /**
   * @return Description of service
   */
  String description();

  /**
   * Execute the item with the given arguments.
   *
   * @param args
   *          command-line arguments to pass to the executed class
   */
  void execute(final String[] args) throws Exception;

}
