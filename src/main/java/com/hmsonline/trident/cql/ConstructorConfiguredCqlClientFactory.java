package com.hmsonline.trident.cql;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryOptions;

public class ConstructorConfiguredCqlClientFactory extends CqlClientFactory {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ConstructorConfiguredCqlClientFactory.class);
    private String[] hosts;
    private String clusterName = null;
    private ConsistencyLevel clusterConsistencyLevel= null;
    private ConsistencyLevel serialConsistencyLevel = null;

    protected static Cluster cluster;
    private final ProtocolOptions.Compression compression;

    public ConstructorConfiguredCqlClientFactory(String hosts) {
        this(hosts, null, ConsistencyLevel.QUORUM, QueryOptions.DEFAULT_SERIAL_CONSISTENCY_LEVEL, ProtocolOptions.Compression.NONE);
    }

    public ConstructorConfiguredCqlClientFactory(String hosts, ConsistencyLevel clusterConsistency) {
        this(hosts, null, clusterConsistency, QueryOptions.DEFAULT_SERIAL_CONSISTENCY_LEVEL, ProtocolOptions.Compression.NONE);
    }

    public ConstructorConfiguredCqlClientFactory(String hosts, String clusterName, ConsistencyLevel clusterConsistency,
                            ConsistencyLevel conditionalUpdateConsistency, ProtocolOptions.Compression compression) {
        this.hosts = hosts.split(",");
        this.clusterConsistencyLevel = clusterConsistency;
        if (conditionalUpdateConsistency != null){
            this.serialConsistencyLevel = conditionalUpdateConsistency;
        }
        if (clusterName != null) {
            this.clusterName = clusterName;
        }
        this.compression = compression;
    }
    
    public Cluster.Builder getClusterBuilder() {

        final List<InetSocketAddress> sockets = new ArrayList<InetSocketAddress>();
        for (String host : hosts) {
            if(StringUtils.contains(host, ":")) {
                String hostParts [] = StringUtils.split(host, ":");
                sockets.add(new InetSocketAddress(hostParts[0], Integer.valueOf(hostParts[1])));
                LOG.debug("Connecting to [" + host + "] with port [" + hostParts[1] + "]");
            } else {
                sockets.add(new InetSocketAddress(host, ProtocolOptions.DEFAULT_PORT));
                LOG.debug("Connecting to [" + host + "] with port [" + ProtocolOptions.DEFAULT_PORT + "]");
            }
        }

        Cluster.Builder builder = Cluster.builder().addContactPointsWithPorts(sockets).withCompression(compression);
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setConsistencyLevel(clusterConsistencyLevel);
        queryOptions.setSerialConsistencyLevel(serialConsistencyLevel);
        builder = builder.withQueryOptions(queryOptions);

        if (StringUtils.isNotEmpty(clusterName)) {
            builder = builder.withClusterName(clusterName);
        }

        return builder;

    }
}
