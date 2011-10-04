package cloudtrace.instrument;

import org.junit.Test;

public class PerformanceTest {
    
    @Test
    public void test() {
        
    }
    
    public static void main(String[] args) {
        
        long now = System.currentTimeMillis();
        for (long i = 0; i < 1000*1000; i++) {
            @SuppressWarnings("unused")
            Long x = new Long(i);
        }
        System.out.println(String.format("Trivial took %d millis", System.currentTimeMillis() - now));
        now = System.currentTimeMillis();
        for (long i = 0; i < 1000*1000; i++)
        {
            Span s = Trace.start("perf");
            s.stop();
        }
        System.out.println(String.format("Span Loop took %d millis", System.currentTimeMillis() - now));
        now = System.currentTimeMillis();
        Trace.on("test");
        for (long i = 0; i < 1000*1000; i++)
        {
            Span s = Trace.start("perf");
            s.stop();
        }
        Trace.off();
        System.out.println(String.format("Trace took %d millis", System.currentTimeMillis() - now));
    }
}
