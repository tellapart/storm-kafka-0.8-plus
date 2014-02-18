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
                port = _stunnelPorts.get(0).intValue();
                _stunnelPorts.remove(0);
                _hostStunnelPortMap.put(host, port);
            }
            return new Broker(host, port);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
