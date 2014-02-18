package storm.kafka;

import backtype.storm.Config;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import storm.kafka.trident.GlobalPartitionInformation;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DynamicStunnelBrokersReaderTest extends DynamicBrokersReadersTestBase {

    private List<Integer> sTunnelPorts;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        sTunnelPorts = new ArrayList<Integer>(Arrays.asList(1234, 1235, 1236));
        conf.put("tellapart.storm.stunnel.ports", new ArrayList(sTunnelPorts));
        dynamicBrokersReader = new DynamicStunnelBrokersReader(conf, server.getConnectString(), MASTER_PATH, TOPIC);
    }

    @Test
    public void testGetBrokerInfo() throws Exception {
        addPartition(0, 0, "localhost", 9092);
        addPartition(1, 1, "another_host", 9092);
        addPartition(2, 2, "yet_another_host", 9092);
        GlobalPartitionInformation brokerInfo = dynamicBrokersReader.getBrokerInfo();

        assertEquals(3, brokerInfo.getOrderedPartitions().size());

        assertTrue(sTunnelPorts.contains(brokerInfo.getBrokerFor(0).port));
        sTunnelPorts.remove(Integer.valueOf(brokerInfo.getBrokerFor(0).port));

        assertTrue(sTunnelPorts.contains(brokerInfo.getBrokerFor(1).port));
        sTunnelPorts.remove(Integer.valueOf(brokerInfo.getBrokerFor(1).port));

        assertTrue(sTunnelPorts.contains(brokerInfo.getBrokerFor(2).port));
        sTunnelPorts.remove(Integer.valueOf(brokerInfo.getBrokerFor(2).port));
    }
}
