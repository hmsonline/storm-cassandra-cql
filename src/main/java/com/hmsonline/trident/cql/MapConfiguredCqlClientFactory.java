package com.hmsonline.trident.cql;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryLogger;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MapConfiguredCqlClientFactory extends CqlClientFactory {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MapConfiguredCqlClientFactory.class);

    public static final String TRIDENT_CASSANDRA_CQL_HOSTS = "trident.cassandra.cql.hosts";
    public static final String TRIDENT_CASSANDRA_COMPRESSION = "trident.cassandra.compression";
    public static final String TRIDENT_CASSANDRA_CONNECT_TIMEOUT = "trident.cassandra.connect.timeout";
    public static final String TRIDENT_CASSANDRA_READ_TIMEOUT = "trident.cassandra.read.timeout";
    public static final String TRIDENT_CASSANDRA_CLUSTER_NAME = "trident.cassandra.cluster.name";
    public static final String TRIDENT_CASSANDRA_LOCAL_DATA_CENTER_NAME = "trident.cassandra.local.data.center.name";
    public static final String TRIDENT_CASSANDRA_CONSISTENCY = "trident.cassandra.consistency";
    public static final String TRIDENT_CASSANDRA_SERIAL_CONSISTENCY = "trident.cassandra.serial.consistency";
    public static final String TRIDENT_CASSANDRA_QUERY_LOGGER_CONSTANT_THRESHOLD = "trident.cassandra.query.logger.constant.threshold";

    final Map<Object,Object> configuration;

    Cluster.Builder builder;

    public MapConfiguredCqlClientFactory(final Map<Object,Object> configuration) {
        this.builder = Cluster.builder();
        this.configuration = configuration;
    }

    private void configureHosts() {
        final String hostConfiguration = (String) configuration.get(TRIDENT_CASSANDRA_CQL_HOSTS);
        final String[] hosts = hostConfiguration.split(",");
        final List<InetSocketAddress> sockets = new ArrayList<InetSocketAddress>();
        for (final String host : hosts) {
            if(StringUtils.contains(host, ":")) {
                final String hostParts [] = StringUtils.split(host, ":");
                sockets.add(new InetSocketAddress(hostParts[0], Integer.valueOf(hostParts[1])));
                LOG.debug("Configuring [" + host + "] with port [" + hostParts[1] + "]");
            } else {
                sockets.add(new InetSocketAddress(host, ProtocolOptions.DEFAULT_PORT));
                LOG.debug("Configuring [" + host + "] with port [" + ProtocolOptions.DEFAULT_PORT + "]");
            }
        }
        builder = builder.addContactPointsWithPorts(sockets);
    }

    private void configureSocketOpts() {
        final String readTimeoutConfiguration = (String) configuration.get(TRIDENT_CASSANDRA_READ_TIMEOUT);
        final String connectTimeoutConfiguration = (String) configuration.get(TRIDENT_CASSANDRA_CONNECT_TIMEOUT);
        final SocketOptions socketOptions = builder.getConfiguration().getSocketOptions();

        if (StringUtils.isNotEmpty(readTimeoutConfiguration)) {
            socketOptions.setReadTimeoutMillis(Integer.parseInt(readTimeoutConfiguration));
        }

        if (StringUtils.isNotEmpty(connectTimeoutConfiguration)) {
            socketOptions.setConnectTimeoutMillis(Integer.parseInt(connectTimeoutConfiguration));
        }

        builder = builder.withSocketOptions(socketOptions);
    }

    private void configureQueryOptions() {

        final String consistencyConfiguration = (String) configuration.get(TRIDENT_CASSANDRA_CONSISTENCY);
        final String serialConsistencyConfiguration = (String) configuration.get(TRIDENT_CASSANDRA_SERIAL_CONSISTENCY);
        final QueryOptions queryOptions = builder.getConfiguration().getQueryOptions();

        if (StringUtils.isNotEmpty(consistencyConfiguration)) {
            queryOptions.setConsistencyLevel(ConsistencyLevel.valueOf(consistencyConfiguration));
        }

        if (StringUtils.isNotEmpty(serialConsistencyConfiguration)) {
            queryOptions.setSerialConsistencyLevel(ConsistencyLevel.valueOf(serialConsistencyConfiguration));
        }

        builder = builder.withQueryOptions(queryOptions);

    }

    private void configureCompression() {
        final String compressionConfiguration = (String) configuration.get(TRIDENT_CASSANDRA_COMPRESSION);
        if (StringUtils.isNotEmpty(compressionConfiguration)) {
            builder = builder.withCompression(ProtocolOptions.Compression.valueOf(compressionConfiguration));
        }
    }

    private void configureOther() {
        final String nameConfiguration = (String) configuration.get(TRIDENT_CASSANDRA_CLUSTER_NAME);
        if (StringUtils.isNotEmpty(nameConfiguration)) {
            builder = builder.withClusterName(nameConfiguration);
        }
    }

    private void configureLoadBalancingPolicy() {
        final String dataCenterNameConfiguration = (String) configuration.get(TRIDENT_CASSANDRA_LOCAL_DATA_CENTER_NAME);
        if (StringUtils.isNotEmpty(dataCenterNameConfiguration)) {
            final LoadBalancingPolicy loadBalancingPolicy = DCAwareRoundRobinPolicy.builder().withLocalDc(dataCenterNameConfiguration).build();
            builder = builder.withLoadBalancingPolicy(loadBalancingPolicy);
        }
    }

    public void configure() {
        configureHosts();
        configureSocketOpts();
        configureQueryOptions();
        configureCompression();
        configureLoadBalancingPolicy();
        configureOther();
    }

    @Override
    protected void prepareCluster(final Cluster cluster) {
        super.prepareCluster(cluster);
        final String threshold = (String) configuration.get(TRIDENT_CASSANDRA_QUERY_LOGGER_CONSTANT_THRESHOLD);
        if (StringUtils.isNotEmpty(threshold)) {
            final QueryLogger logger = QueryLogger.builder()
                    .withConstantThreshold(Long.parseLong(threshold))
                    .build();
            cluster.register(logger);
        }
    }

    public Cluster.Builder getClusterBuilder() {
        configure();
        return builder;
    }
}
