package com.hmsonline.trident.cql;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolOptions;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.net.InetSocketAddress;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class ConstructorConfiguredCqlClientFactoryTest extends CqlClientFactoryTest {

    @Test
    public void testGetCluster() {
        final CqlClientFactory factory =
                new ConstructorConfiguredCqlClientFactory(HOSTS,
                                                          CLUSTER_NAME,
                                                          DEFAULT_CONSISTENCY_LEVEL,
                                                          DEFAULT_SERIAL_CONSISTENCY_LEVEL,
                                                          COMPRESSION);

        final Cluster.Builder clusterBuilder = factory.getClusterBuilder();
        Assert.assertEquals(CLUSTER_NAME, clusterBuilder.getClusterName());
        final InetSocketAddress first = clusterBuilder.getContactPoints().get(0);
        final InetSocketAddress second = clusterBuilder.getContactPoints().get(1);
        Assert.assertEquals("localhost", first.getHostName());
        Assert.assertEquals(9042, first.getPort());
        Assert.assertEquals("remotehost", second.getHostName());
        Assert.assertEquals(1234, second.getPort());
    }

}