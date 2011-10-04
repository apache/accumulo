package cloudtrace.instrument;

/**
 * Wrap a Runnable with a Span that survives a change in threads.
 * 
 */
public class TraceRunnable implements Runnable, Comparable<TraceRunnable> {
    
    private final Span parent;
    private final Runnable runnable;

    public TraceRunnable(Runnable runnable) { 
        this(Trace.currentTrace(), runnable); 
    }
        
    public TraceRunnable(Span parent, Runnable runnable) {
        this.parent = parent;
        this.runnable = runnable;
    }

    @Override
    public void run() {
        if (parent != null) {
            Span chunk = Trace.startThread(parent, Thread.currentThread().getName());
            try {
                runnable.run();
            } finally {
                Trace.endThread(chunk);
            }
        } else {
            runnable.run();
        }
    }

	@SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
	public int compareTo(TraceRunnable o) {
		return ((Comparable)this.runnable).compareTo(o.runnable);
	}
}
