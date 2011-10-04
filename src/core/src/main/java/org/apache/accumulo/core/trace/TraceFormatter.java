package org.apache.accumulo.core.trace;

import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TMemoryInputTransport;

import cloudtrace.thrift.RemoteSpan;

/**
 * A formatter than can be used in the shell to display trace information. 
 *
 */
public class TraceFormatter implements Formatter {
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
    private final static Text SPAN_CF = new Text("span");

    private Iterator<Entry<Key, Value>> scanner;
    private boolean                     printTimeStamps;

    public static RemoteSpan getRemoteSpan(Entry<Key, Value> entry) {
        TMemoryInputTransport transport = new TMemoryInputTransport(entry.getValue().get());
        TCompactProtocol protocol = new TCompactProtocol(transport);
        RemoteSpan span = new RemoteSpan();
        try {
            span.read(protocol);
        } catch (TException ex) {
            throw new RuntimeException(ex);
        }
        return span;
    }

    @Override
    public boolean hasNext() {
        return scanner.hasNext();
    }

    @Override
    public String next() {
        Entry<Key, Value> next = scanner.next();
        if (next.getKey().getColumnFamily().equals(SPAN_CF)) {
            StringBuilder result = new StringBuilder();
            RemoteSpan span = getRemoteSpan(next);
            result.append("----------------------\n");
            result.append(String.format(" %12s:%s\n", "name", span.description));
            result.append(String.format(" %12s:%s\n", "trace", Long.toHexString(span.traceId)));
            result.append(String.format(" %12s:%s\n", "loc", span.svc + "@" + span.sender));
            result.append(String.format(" %12s:%s\n", "span", Long.toHexString(span.spanId)));
            result.append(String.format(" %12s:%s\n", "parent", Long.toHexString(span.parentId)));
            result.append(String.format(" %12s:%s\n", "start", DATE_FORMAT.format(span.start)));
            result.append(String.format(" %12s:%s\n", "ms", span.stop - span.start));
            for (Entry<String, String> entry : span.data.entrySet()) {
                result.append(String.format(" %12s:%s\n", entry.getKey(), entry.getValue()));
            }

            if (printTimeStamps) {
                result.append(String.format(" %-12:%d\n", "timestamp", next.getKey().getTimestamp()));
            }
            return result.toString();
        }
        return DefaultFormatter.formatEntry(next, printTimeStamps);
    }

    @Override
    public void remove() {
        throw new NotImplementedException();
    }

    @Override
    public void initialize(Iterable<Entry<Key, Value>> scanner, boolean printTimestamps) {
        this.scanner = scanner.iterator();
        this.printTimeStamps = printTimestamps;
    }
}
