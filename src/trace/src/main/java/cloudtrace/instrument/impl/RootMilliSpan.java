package cloudtrace.instrument.impl;


/**
 * Span that roots the span tree in a process, but perhaps not the whole trace.
 *
 */
public class RootMilliSpan extends MilliSpan {
    
    final long traceId;
    final long parentId;

    @Override
    public long traceId() {
        return traceId;
    }

    public RootMilliSpan(String description, long traceId, long spanId, long parentId) {
        super(description, spanId, null);
        this.traceId = traceId;
        this.parentId = parentId;
    }
    
    public long parentId() {
        return parentId;
    }
   
}
