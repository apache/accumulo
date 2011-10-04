package cloudtrace.instrument.receivers;

import java.util.Map;

/**
 * The collector within a process that is the destination of Spans when a trace is running.
 */
public interface SpanReceiver {
    void span(long traceId, long spanId, long parentId, long start, long stop, String description,
                     Map<String, String> data);
    void flush();
}
