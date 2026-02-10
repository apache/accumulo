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
package org.apache.accumulo.core.cli;

import org.apache.accumulo.start.spi.KeywordExecutable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

public abstract class ClientKeywordExecutable<O extends ClientOpts> implements KeywordExecutable {

  private final O options;

  public ClientKeywordExecutable(O options) {
    this.options = options;
  }

  @Override
  public final void execute(String[] args) throws Exception {
    JCommander cl = new JCommander(this.options);
    cl.setProgramName(
        "accumulo " + (usageGroup().key().isBlank() ? "" : usageGroup().key() + " ") + keyword());
    try {
      cl.parse(args);
    } catch (ParameterException e) {
      cl.usage();
      return;
    }

    if (this.options.help) {
      cl.usage();
      return;
    }
    execute(cl, options);
  }

  public abstract void execute(JCommander cl, O options) throws Exception;

}
