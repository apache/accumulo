package cloudtrace.instrument.impl;

import java.util.Collections;
import java.util.Map;

import cloudtrace.instrument.Span;

/**
 * A Span that does nothing.  Used to avoid returning and checking for nulls when we are not tracing.
 *
 */
public class NullSpan implements Span {
    
    public NullSpan() {
    }

    @Override
    public long accumulatedMillis() {
        return 0;
    }

    @Override
    public String description() {
        return "NullSpan";
    }

    @Override
    public long getStartTimeMillis() {
        return 0;
    }

    @Override
    public long getStopTimeMillis() {
        return 0;
    }

    @Override
    public Span parent() {
        return null;
    }

    @Override
    public long parentId() {
        return -1;
    }

    @Override
    public boolean running() {
        return false;
    }

    @Override
    public long spanId() {
        return -1;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public long traceId() {
        return -1;
    }

    @Override
    public Span child(String description) {
        return this;
    }

    @Override
    public void data(String key, String value) {
    }

    @Override
    public String toString() {
        return "Not Tracing";
    }

    @Override
    public Map<String, String> getData() {
        return Collections.emptyMap();
    }
    
}
