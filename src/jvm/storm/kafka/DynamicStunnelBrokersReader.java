package storm.kafka;

import org.json.simple.JSONValue;

import java.io.UnsupportedEncodingException;
import java.util.*;

public class DynamicStunnelBrokersReader extends DynamicBrokersReader {

    private List<Integer> _stunnelPorts;
    private Map<String, Integer> _hostStunnelPortMap;

    public DynamicStunnelBrokersReader(Map conf, String zkStr, String zkPath, String topic) {
        super(conf, zkStr, zkPath, topic);
        _stunnelPorts = (List<Integer>)conf.get("tellapart.storm.stunnel.ports");
        _hostStunnelPortMap = new HashMap<String, Integer>();
    }

    @Override
    protected Broker getBrokerHost(byte[] contents) {
        try {
            Map<Object, Object> value = (Map<Object, Object>) JSONValue.parse(new String(contents, "UTF-8"));
            String host = (String) value.get("host");

            Integer port;
            if (_hostStunnelPortMap.containsKey(host)) {
                port = _hostStunnelPortMap.get(host);
            } else {
                // If there are no more stunnel ports to be assigned, throw an error.
                if (_stunnelPorts.isEmpty()) {
                    throw new RuntimeException("No remaining stunnel ports to assign to brokers.");
                }
                port = _stunnelPorts.get(0);
                _stunnelPorts.remove(0);
                _hostStunnelPortMap.put(host, port);
            }
            // Always return 127.0.0.1 here so that stunnel on the client side can be utilized.
            return new Broker("127.0.0.1", port);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
