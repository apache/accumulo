package org.apache.accumulo.server.trace;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;

import cloudtrace.instrument.Span;
import cloudtrace.instrument.Trace;

public class TraceFSDataInputStream extends FSDataInputStream {
    @Override
    public synchronized void seek(long desired) throws IOException {
        Span span = Trace.start("FSDataInputStream.seek");
        try {
            impl.seek(desired);
        } finally {
            span.stop();
        }
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
            throws IOException {
        Span span = Trace.start("FSDataInputStream.read");
        if (Trace.isTracing())
            span.data("length", Integer.toString(length));
        try {
            return impl.read(position, buffer, offset, length);
        } finally {
            span.stop();
        }
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException {
        Span span = Trace.start("FSDataInputStream.readFully");
        if (Trace.isTracing())
            span.data("length", Integer.toString(length));
        try {
            impl.readFully(position, buffer, offset, length);
        } finally {
            span.stop();
        }
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        Span span = Trace.start("FSDataInputStream.readFully");
        if (Trace.isTracing())
            span.data("length", Integer.toString(buffer.length));
        try {
            impl.readFully(position, buffer);
        } finally {
            span.stop();
        }
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        Span span = Trace.start("FSDataInputStream.seekToNewSource");
        try {
            return impl.seekToNewSource(targetPos);
        } finally {
            span.stop();
        }
    }

    private final FSDataInputStream impl;

    public TraceFSDataInputStream(FSDataInputStream in) throws IOException {
        super(in);
        impl = in;
    }

}
