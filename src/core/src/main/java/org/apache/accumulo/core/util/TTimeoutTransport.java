package org.apache.accumulo.core.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.spi.SelectorProvider;

import org.apache.hadoop.net.SocketInputStream;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

public class TTimeoutTransport {
    
    public static TTransport create(SocketAddress addr, long timeoutMillis) throws IOException {
        Socket socket = SelectorProvider.provider().openSocketChannel().socket();
        socket.setSoLinger(false, 0);
        socket.setTcpNoDelay(true);
        socket.connect(addr);
        InputStream input = new BufferedInputStream(new SocketInputStream(socket, timeoutMillis), 1024*10);
        OutputStream output = new BufferedOutputStream(new SocketOutputStream(socket, timeoutMillis), 1024*10);
        return new TIOStreamTransport(input, output);
    }
}
