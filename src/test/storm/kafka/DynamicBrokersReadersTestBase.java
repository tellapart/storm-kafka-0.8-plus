package storm.kafka;

import backtype.storm.Config;
import com.apple.laf.AquaButtonBorder;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.TestingServer;
import com.netflix.curator.utils.ZKPaths;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

public class DynamicBrokersReadersTestBase {

    protected static final String MASTER_PATH = "/brokers";
    protected static final String TOPIC = "testing";
    protected TestingServer server;
    private CuratorFramework zookeeper;
    protected DynamicBrokersReader dynamicBrokersReader;
    protected Map conf;

    @Before
    public void setUp() throws Exception {
        server = new TestingServer();
        String connectionString = server.getConnectString();
        conf = new HashMap();
        conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 1000);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 4);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 5);
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zookeeper = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        dynamicBrokersReader = new DynamicBrokersReader(conf, connectionString, MASTER_PATH, TOPIC);
        zookeeper.start();
    }

    @After
    public void tearDown() {
        server.close();
    }

    protected void addPartition(int id, String host, int port) throws Exception {
        writePartitionId(id);
        writeLeader(id, 0);
        writeLeaderDetails(0, host, port);
    }

    protected void addPartition(int id, int leader, String host, int port) throws Exception {
        writePartitionId(id);
        writeLeader(id, leader);
        writeLeaderDetails(leader, host, port);
    }

    protected void writePartitionId(int id) throws Exception {
        String path = dynamicBrokersReader.partitionPath();
        writeDataToPath(path, ("" + id));
    }

    protected void writeDataToPath(String path, String data) throws Exception {
        ZKPaths.mkdirs(zookeeper.getZookeeperClient().getZooKeeper(), path);
        zookeeper.setData().forPath(path, data.getBytes());
    }

    protected void writeLeader(int id, int leaderId) throws Exception {
        String path = dynamicBrokersReader.partitionPath() + "/" + id + "/state";
        String value = " { \"controller_epoch\":4, \"isr\":[ 1, 0 ], \"leader\":" + leaderId + ", \"leader_epoch\":1, \"version\":1 }";
        writeDataToPath(path, value);
    }

    protected void writeLeaderDetails(int leaderId, String host, int port) throws Exception {
        String path = dynamicBrokersReader.brokerPath() + "/" + leaderId;
        String value = "{ \"host\":\"" + host + "\", \"jmx_port\":9999, \"port\":" + port + ", \"version\":1 }";
        writeDataToPath(path, value);
    }

}
