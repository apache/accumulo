package cloudtrace.instrument;

import java.util.concurrent.Callable;

/**
 * Wrap a Callable with a Span that survives a change in threads.
 * 
 */
public class TraceCallable<V> implements Callable<V> {
    private final Callable<V> impl;
    private final Span parent;
    
    TraceCallable(Callable<V> impl) {
       this(Trace.currentTrace(), impl);
    }

    TraceCallable(Span parent, Callable<V> impl) {
        this.impl = impl;
        this.parent = parent;
    }

    @Override
    public V call() throws Exception {
        if (parent != null) {
            Span chunk = Trace.startThread(parent, Thread.currentThread().getName());
            try {
                return impl.call();
            } finally {
                Trace.endThread(chunk);
            }
        } else {
            return impl.call();
        }
    }
}
