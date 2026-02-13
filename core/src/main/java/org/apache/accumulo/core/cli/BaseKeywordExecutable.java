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

public abstract class BaseKeywordExecutable<OPTS extends Help> implements KeywordExecutable {

  private final OPTS options;

  protected BaseKeywordExecutable(OPTS options) {
    this.options = options;
  }

  @Override
  public final void execute(String[] args) throws Exception {
    JCommander cl = new JCommander(this.options);
    cl.setProgramName("accumulo "
        + (commandGroup().key().isBlank() ? "" : commandGroup().key() + " ") + keyword());
    try {
      cl.parse(args);
    } catch (ParameterException e) {
      cl.usage();
      return;
    }
    this.options.validateArgs();
    if (this.options.help) {
      cl.usage();
      return;
    }
    doExecute(cl, options);
  }

  // This method exists so that subclasses can override and perform
  // pre and post-execute operations if needed.
  public void doExecute(JCommander cl, OPTS options) throws Exception {
    execute(cl, options);
  }

  public abstract void execute(JCommander cl, OPTS options) throws Exception;

}
