package cloudtrace.instrument;

import java.util.Map;

/**
 * Base interface for gathering and reporting statistics about a block of execution.
 */
public interface Span {
    public static final long ROOT_SPAN_ID = 0;

    /** Begin gathering timing information */
    void start();
    /** The block has completed, stop the clock */
    void stop();
    /** Get the start time, in milliseconds */
    long getStartTimeMillis();
    /** Get the stop time, in milliseconds */
    long getStopTimeMillis();
    /** Return the total amount of time elapsed since start was called, if running, or difference between stop and start */ 
    long accumulatedMillis();
    /** Has the span been started and not yet stopped? */
    boolean running();
    /** Return a textual description of this span */
    String description();
    /** A pseudo-unique (random) number assigned to this span instance */
    long spanId();
    /** The parent span: returns null if this is the root span */
    Span parent();
    /** A pseudo-unique (random) number assigned to the trace associated with this span */
    long traceId();
    /** Create a child span of this span with the given description */
    Span child(String description);
    @Override
    String toString();
    /** Return the pseudo-unique (random) number of the parent span, returns ROOT_SPAN_ID if this is the root span */
    long parentId();
    /** Add data associated with this span */
    void data(String key, String value);
    /** Get data associated with this span (read only) */
    Map<String, String> getData();
}
