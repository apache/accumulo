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
package org.apache.accumulo.test.rpc;

import org.apache.accumulo.test.rpc.thrift.SimpleThriftService;
import org.apache.thrift.TException;

public class SimpleThriftServiceHandler implements SimpleThriftService.Iface {

  private void setProp(String method, String value) {
    System.setProperty(this.getClass().getSimpleName() + "." + method, value);
  }

  @Override
  public String echoFail(String value) throws TException {
    setProp("echoFail", value);
    throw new TException(new UnsupportedOperationException(value));
  }

  @Override
  public String echoRuntimeFail(String value) {
    setProp("echoRuntimeFail", value);
    throw new UnsupportedOperationException(value);
  }

  @Override
  public String echoPass(String value) {
    setProp("echoPass", value);
    return value;
  }

  @Override
  public void onewayFail(String value) throws TException {
    setProp("onewayFail", value);
    throw new TException(new UnsupportedOperationException(value));
  }

  @Override
  public void onewayRuntimeFail(String value) {
    setProp("onewayRuntimeFail", value);
    throw new UnsupportedOperationException(value);
  }

  @Override
  public void onewayPass(String value) {
    setProp("onewayPass", value);
  }

}
