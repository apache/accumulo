package org.apache.accumulo.core.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.impl.thrift.ThriftTest;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.Test;

public class TestThrift1474 {
  
  static class TestServer implements ThriftTest.Iface {

    @Override
    public boolean success() throws TException {
      return true;
    }

    @Override
    public boolean fails() throws TException {
      return false;
    }

    @Override
    public boolean throwsError() throws ThriftSecurityException, TException {
      throw new ThriftSecurityException();
    }
    
  }
  
  @Test
  public void test() throws IOException, TException, InterruptedException {
    TServerSocket serverTransport = new TServerSocket(0);
    serverTransport.listen();
    int port = serverTransport.getServerSocket().getLocalPort();
    TestServer handler = new TestServer();
    ThriftTest.Processor<ThriftTest.Iface> processor = new ThriftTest.Processor<ThriftTest.Iface>(handler);
    
    TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport);
    args.stopTimeoutVal = 10;
    args.stopTimeoutUnit = TimeUnit.MILLISECONDS;
    final TServer server = new TThreadPoolServer(args.processor(processor));
    Thread thread = new Thread() {
      public void run() {
        server.serve();
      }
    };
    thread.start();
    while (!server.isServing()) {
      UtilWaitThread.sleep(10);
    }
    
    TTransport transport = new TSocket("localhost", port);
    transport.open();
    TProtocol protocol = new TBinaryProtocol(transport);
    ThriftTest.Client client = new ThriftTest.Client(protocol);
    assertTrue(client.success());
    assertFalse(client.fails());
    try {
      client.throwsError();
      fail("no exception thrown");
    } catch (ThriftSecurityException ex) {
      // expected
    }
    server.stop();
    thread.join();
  }
  
}
