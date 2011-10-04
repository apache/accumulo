package org.apache.accumulo.core.trace;

import java.util.Collection;

import cloudtrace.thrift.RemoteSpan;

public interface SpanTreeVisitor {
    void visit(int level, RemoteSpan parent, RemoteSpan node, Collection<RemoteSpan> children);
}