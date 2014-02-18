package storm.kafka;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import storm.kafka.trident.GlobalPartitionInformation;

import static org.junit.Assert.assertEquals;

/**
 * Date: 16/05/2013
 * Time: 20:35
 */
public class DynamicBrokersReaderTest extends DynamicBrokersReadersTestBase {

    @Test
    public void testGetBrokerInfo() throws Exception {
        String host = "localhost";
        int port = 9092;
        int partition = 0;
        addPartition(partition, host, port);
        GlobalPartitionInformation brokerInfo = dynamicBrokersReader.getBrokerInfo();
        assertEquals(1, brokerInfo.getOrderedPartitions().size());
        assertEquals(port, brokerInfo.getBrokerFor(partition).port);
        assertEquals(host, brokerInfo.getBrokerFor(partition).host);
    }


    @Test
    public void testMultiplePartitionsOnDifferentHosts() throws Exception {
        String host = "localhost";
        int port = 9092;
        int secondPort = 9093;
        int partition = 0;
        int secondPartition = partition + 1;
        addPartition(partition, 0, host, port);
        addPartition(secondPartition, 1, host, secondPort);

        GlobalPartitionInformation brokerInfo = dynamicBrokersReader.getBrokerInfo();
        assertEquals(2, brokerInfo.getOrderedPartitions().size());

        assertEquals(port, brokerInfo.getBrokerFor(partition).port);
        assertEquals(host, brokerInfo.getBrokerFor(partition).host);

        assertEquals(secondPort, brokerInfo.getBrokerFor(secondPartition).port);
        assertEquals(host, brokerInfo.getBrokerFor(secondPartition).host);
    }


    @Test
    public void testMultiplePartitionsOnSameHost() throws Exception {
        String host = "localhost";
        int port = 9092;
        int partition = 0;
        int secondPartition = partition + 1;
        addPartition(partition, 0, host, port);
        addPartition(secondPartition, 0, host, port);

        GlobalPartitionInformation brokerInfo = dynamicBrokersReader.getBrokerInfo();
        assertEquals(2, brokerInfo.getOrderedPartitions().size());

        assertEquals(port, brokerInfo.getBrokerFor(partition).port);
        assertEquals(host, brokerInfo.getBrokerFor(partition).host);

        assertEquals(port, brokerInfo.getBrokerFor(secondPartition).port);
        assertEquals(host, brokerInfo.getBrokerFor(secondPartition).host);
    }

    @Test
    public void testSwitchHostForPartition() throws Exception {
        String host = "localhost";
        int port = 9092;
        int partition = 0;
        addPartition(partition, host, port);
        GlobalPartitionInformation brokerInfo = dynamicBrokersReader.getBrokerInfo();
        assertEquals(port, brokerInfo.getBrokerFor(partition).port);
        assertEquals(host, brokerInfo.getBrokerFor(partition).host);

        String newHost = host + "switch";
        int newPort = port + 1;
        addPartition(partition, newHost, newPort);
        brokerInfo = dynamicBrokersReader.getBrokerInfo();
        assertEquals(newPort, brokerInfo.getBrokerFor(partition).port);
        assertEquals(newHost, brokerInfo.getBrokerFor(partition).host);
    }
}
