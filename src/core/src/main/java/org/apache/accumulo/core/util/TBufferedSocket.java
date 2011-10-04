package org.apache.accumulo.core.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;

import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TSocket;

public class TBufferedSocket extends TIOStreamTransport {
    
    String client;
    
    public TBufferedSocket(TSocket sock, int bufferSize) throws IOException {
        super(new BufferedInputStream(sock.getSocket().getInputStream(), bufferSize),
                new BufferedOutputStream(sock.getSocket().getOutputStream(), bufferSize));
        client = sock.getSocket().getInetAddress().getHostAddress()+":"+sock.getSocket().getPort();
    }
    
    public String getClientString() {
        return client;
    }

}
