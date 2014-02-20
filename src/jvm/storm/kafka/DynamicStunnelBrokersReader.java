package storm.kafka;

import org.json.simple.JSONValue;

import java.io.UnsupportedEncodingException;
import java.util.*;

public class DynamicStunnelBrokersReader extends DynamicBrokersReader {

    private Map<String, Number> _hostToStunnelPort;

    public DynamicStunnelBrokersReader(Map conf, String zkStr, String zkPath, String topic) {
        super(conf, zkStr, zkPath, topic);
        _hostToStunnelPort = new HashMap<String, Number>((Map<String, Number>)conf.get("tellapart.storm.stunnel.host_to_stunnel_port"));
    }

    @Override
    protected Broker getBrokerHost(byte[] contents) {
        try {
            Map<Object, Object> value = (Map<Object, Object>) JSONValue.parse(new String(contents, "UTF-8"));
            String host = (String) value.get("host");
            Integer port = (_hostToStunnelPort.get(host)).intValue();
            // Always return 127.0.0.1 here so that stunnel on the client side can be utilized.
            return new Broker("127.0.0.1", port);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
