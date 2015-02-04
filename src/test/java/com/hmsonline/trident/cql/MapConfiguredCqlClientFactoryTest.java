package com.hmsonline.trident.cql;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import junit.framework.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class MapConfiguredCqlClientFactoryTest extends CqlClientFactoryTest {

    @Test
    public void testGetClusterBuilder() throws Exception {
        final Map<String, String> configuration = new HashMap<String,String>();
        configuration.put(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_CQL_HOSTS, HOSTS);
        configuration.put(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_CLUSTER_NAME, CLUSTER_NAME);
        configuration.put(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_READ_TIMEOUT, READ_TIMEOUT);
        configuration.put(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_CONNECT_TIMEOUT, CONNECT_TIMEOUT);
        configuration.put(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_LOCAL_DATA_CENTER_NAME, DATA_CENTER_NAME);
        configuration.put(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_CONSISTENCY, DEFAULT_CONSISTENCY_LEVEL.name());
        configuration.put(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_SERIAL_CONSISTENCY, DEFAULT_SERIAL_CONSISTENCY_LEVEL.name());

        final CqlClientFactory factory =
                new MapConfiguredCqlClientFactory(configuration);

        final Cluster.Builder clusterBuilder = factory.getClusterBuilder();
        Assert.assertEquals(CLUSTER_NAME, clusterBuilder.getClusterName());
        final InetSocketAddress first = clusterBuilder.getContactPoints().get(0);
        final InetSocketAddress second = clusterBuilder.getContactPoints().get(1);
        Assert.assertEquals("localhost", first.getHostName());
        Assert.assertEquals(9042, first.getPort());
        Assert.assertEquals("remotehost", second.getHostName());
        Assert.assertEquals(1234, second.getPort());
        Assert.assertEquals(Integer.parseInt(CONNECT_TIMEOUT), clusterBuilder.getConfiguration().getSocketOptions().getConnectTimeoutMillis());
        Assert.assertEquals(Integer.parseInt(READ_TIMEOUT), clusterBuilder.getConfiguration().getSocketOptions().getReadTimeoutMillis());
        Assert.assertEquals(DEFAULT_CONSISTENCY_LEVEL, clusterBuilder.getConfiguration().getQueryOptions().getConsistencyLevel());
        Assert.assertEquals(DEFAULT_SERIAL_CONSISTENCY_LEVEL, clusterBuilder.getConfiguration().getQueryOptions().getSerialConsistencyLevel());
        Assert.assertEquals(ProtocolOptions.Compression.NONE, clusterBuilder.getConfiguration().getProtocolOptions().getCompression());
    }

    @Test(expected = IllegalStateException.class)
    public void testCompression() {
        final Map<String, String> configuration = new HashMap<String,String>();
        configuration.put(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_CQL_HOSTS, HOSTS);
        configuration.put(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_COMPRESSION, COMPRESSION.name());
        final CqlClientFactory factory =
                new MapConfiguredCqlClientFactory(configuration);
        factory.getClusterBuilder().getConfiguration().getProtocolOptions().getCompression();
        // We're testing for the exception here since we don't have LZ4 on our classpath.
        // Assert.assertEquals(COMPRESSION, clusterBuilder.getConfiguration().getProtocolOptions().getCompression());
    }
}