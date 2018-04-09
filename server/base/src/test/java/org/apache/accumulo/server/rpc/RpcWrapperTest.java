/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.rpc;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.trace.wrappers.RpcServerInvocationHandler;
import org.apache.thrift.ProcessFunction;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Verification that RpcWrapper correctly mangles Exceptions to work around Thrift.
 */
public class RpcWrapperTest {

  private static final String RTE_MESSAGE = "RpcWrapperTest's RuntimeException Message";

  /**
   * Given a method name and whether or not the method is oneway, construct a ProcessFunction.
   *
   * @param methodName
   *          The service method name.
   * @param isOneway
   *          Is the method oneway.
   * @return A ProcessFunction.
   */
  private fake_proc<FakeService> createProcessFunction(String methodName, boolean isOneway) {
    return new fake_proc<>(methodName, isOneway);
  }

  @Test
  public void testSomeOnewayMethods() {
    Map<String,ProcessFunction<FakeService,?>> procs = new HashMap<>();
    procs.put("foo", createProcessFunction("foo", true));
    procs.put("foobar", createProcessFunction("foobar", false));
    procs.put("bar", createProcessFunction("bar", true));
    procs.put("barfoo", createProcessFunction("barfoo", false));

    Set<String> onewayMethods = RpcWrapper.getOnewayMethods(procs);
    Assert.assertEquals(Sets.newHashSet("foo", "bar"), onewayMethods);
  }

  @Test
  public void testNoOnewayMethods() {
    Map<String,ProcessFunction<FakeService,?>> procs = new HashMap<>();
    procs.put("foo", createProcessFunction("foo", false));
    procs.put("foobar", createProcessFunction("foobar", false));
    procs.put("bar", createProcessFunction("bar", false));
    procs.put("barfoo", createProcessFunction("barfoo", false));

    Set<String> onewayMethods = RpcWrapper.getOnewayMethods(procs);
    Assert.assertEquals(Collections.<String> emptySet(), onewayMethods);
  }

  @Test
  public void testAllOnewayMethods() {
    Map<String,ProcessFunction<FakeService,?>> procs = new HashMap<>();
    procs.put("foo", createProcessFunction("foo", true));
    procs.put("foobar", createProcessFunction("foobar", true));
    procs.put("bar", createProcessFunction("bar", true));
    procs.put("barfoo", createProcessFunction("barfoo", true));

    Set<String> onewayMethods = RpcWrapper.getOnewayMethods(procs);
    Assert.assertEquals(Sets.newHashSet("foo", "foobar", "bar", "barfoo"), onewayMethods);
  }

  @Test
  public void testNoExceptionWrappingForOneway() throws Throwable {
    final Object[] args = new Object[0];

    final FakeService impl = new FakeServiceImpl();

    // "short" names throw RTEs and are oneway, while long names do not throw exceptions and are not
    // oneway.
    RpcServerInvocationHandler<FakeService> handler = RpcWrapper.getInvocationHandler(impl,
        Sets.newHashSet("foo", "bar"));

    // Should throw an exception, but not be wrapped because the method is oneway
    try {
      handler.invoke(impl, FakeServiceImpl.class.getMethod("foo"), args);
      Assert.fail("Expected an exception");
    } catch (RuntimeException e) {
      Assert.assertEquals(RTE_MESSAGE, e.getMessage());
    }

    // Should not throw an exception
    handler.invoke(impl, FakeServiceImpl.class.getMethod("foobar"), args);
  }

  @Test
  public void testExceptionWrappingForNonOneway() throws Throwable {
    final Object[] args = new Object[0];

    final FakeService impl = new FakeServiceImpl();

    // "short" names throw RTEs and are not oneway, while long names do not throw exceptions and are
    // oneway.
    RpcServerInvocationHandler<FakeService> handler = RpcWrapper.getInvocationHandler(impl,
        Sets.newHashSet("foobar", "barfoo"));

    // Should throw an exception, but not be wrapped because the method is oneway
    try {
      handler.invoke(impl, FakeServiceImpl.class.getMethod("foo"), args);
      Assert.fail("Expected an exception");
    } catch (TException e) {
      // The InvocationHandler should take the exception from the RTE and make it a TException
      Assert.assertEquals(RTE_MESSAGE, e.getMessage());
    }

    // Should not throw an exception
    handler.invoke(impl, FakeServiceImpl.class.getMethod("foobar"), args);
  }

  //
  // Some hacked together classes/interfaces that mimic what Thrift is doing.
  //

  /**
   * Some fake fields for our fake arguments.
   */
  private static class fake_fields implements org.apache.thrift.TFieldIdEnum {
    @Override
    public short getThriftFieldId() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getFieldName() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * A fake thrift service
   */
  interface FakeService {
    void foo();

    String foobar();

    int bar();

    long barfoo();
  }

  /**
   * An implementation of the fake thrift service. The "short" names throw RTEs, while long names do
   * not.
   */
  public static class FakeServiceImpl implements FakeService {
    @Override
    public void foo() {
      throw new RuntimeException(RTE_MESSAGE);
    }

    @Override
    public String foobar() {
      return "";
    }

    @Override
    public int bar() {
      throw new RuntimeException(RTE_MESSAGE);
    }

    @Override
    public long barfoo() {
      return 0;
    }
  }

  /**
   * A fake ProcessFunction implementation for testing that allows injection of method name and
   * oneway.
   */
  private static class fake_proc<I extends FakeService>
      extends org.apache.thrift.ProcessFunction<I,foo_args> {
    final private boolean isOneway;

    public fake_proc(String methodName, boolean isOneway) {
      super(methodName);
      this.isOneway = isOneway;
    }

    @Override
    protected boolean isOneway() {
      return isOneway;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public TBase getResult(I iface, foo_args args) throws TException {
      throw new UnsupportedOperationException();
    }

    @Override
    public foo_args getEmptyArgsInstance() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Fake arguments for our fake service.
   */
  private static class foo_args implements org.apache.thrift.TBase<foo_args,fake_fields> {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean equals(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(foo_args o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void read(TProtocol iprot) throws TException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void write(TProtocol oprot) throws TException {
      throw new UnsupportedOperationException();
    }

    @Override
    public fake_fields fieldForId(int fieldId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSet(fake_fields field) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object getFieldValue(fake_fields field) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setFieldValue(fake_fields field, Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public TBase<foo_args,fake_fields> deepCopy() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }
  }
}
