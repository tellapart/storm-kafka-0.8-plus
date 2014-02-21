package storm.kafka;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import storm.kafka.trident.GlobalPartitionInformation;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DynamicStunnelBrokersReaderTest extends DynamicBrokersReadersTestBase {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private Map<String, Number> hostToStunnelPort;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        hostToStunnelPort = new HashMap<String, Number>();
        hostToStunnelPort.put("localhost", 1233);
        hostToStunnelPort.put("another_host", 1234);
        hostToStunnelPort.put("yet_another_host", 1235);
        conf.put("tellapart.storm.stunnel.host_to_stunnel_port", hostToStunnelPort);
        dynamicBrokersReader = new DynamicStunnelBrokersReader(conf, server.getConnectString(), MASTER_PATH, TOPIC);
    }

    @Test
    public void testGetBrokerInfo() throws Exception {
        addPartition(0, 0, "localhost", 9092);
        addPartition(1, 1, "another_host", 9092);
        addPartition(2, 2, "yet_another_host", 9092);
        GlobalPartitionInformation brokerInfo = dynamicBrokersReader.getBrokerInfo();

        assertEquals(3, brokerInfo.getOrderedPartitions().size());
        assertEquals(1233, brokerInfo.getBrokerFor(0).port);
        assertEquals(1234, brokerInfo.getBrokerFor(1).port);
        assertEquals(1235, brokerInfo.getBrokerFor(2).port);
    }

    @Test
    public void testGetBrokerInfoMissingHostname() throws Exception {
        addPartition(0, 0, "some_random_host", 9092);
        exception.expect(RuntimeException.class);
        GlobalPartitionInformation brokerInfo = dynamicBrokersReader.getBrokerInfo();
    }


}
