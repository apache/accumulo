package cloudtrace.instrument.receivers;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import cloudtrace.thrift.RemoteSpan;
import cloudtrace.thrift.SpanReceiver.Client;

/**
 * Send Span data to a destination using thrift.
 */
public class SendSpansViaThrift extends AsyncSpanReceiver<String, Client> {

    private static final org.apache.log4j.Logger log =  org.apache.log4j.Logger.getLogger(SendSpansViaThrift.class);

    private static final String THRIFT = "thrift://";
    
    public SendSpansViaThrift(String host, String service, long millis) {
        super(host, service, millis);
    }
    
    @Override
    protected Client createDestination(String destination) throws Exception {
        try {
            String[] hostAddr = destination.split(":", 2);
            log.debug("Connecting to " + hostAddr[0] + ":" + hostAddr[1]);
            InetSocketAddress addr = new InetSocketAddress(hostAddr[0], Integer.parseInt(hostAddr[1]));
            Socket sock = new Socket();
            sock.connect(addr);
            TTransport transport = new TSocket(sock);
            TProtocol prot = new TBinaryProtocol(transport);
            return new Client(prot);
        } catch (Exception ex) {
            log.error(ex, ex);
            return null;
        }
    }
    
    @Override
    protected void send(Client client, RemoteSpan s) throws Exception {
    	client.span(s);
    }

    protected String getSpanKey(Map<String, String> data) {
        String dest = data.get("dest");
        if (dest != null && dest.startsWith(THRIFT))
        {
            String hostAddress = dest.substring(THRIFT.length());
            String[] hostAddr = hostAddress.split(":", 2);
            if (hostAddr.length == 2)
            {
                return hostAddress;
            }
        }
        return null;
    }

}
