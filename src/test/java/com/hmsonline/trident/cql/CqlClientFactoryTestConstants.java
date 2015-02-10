package com.hmsonline.trident.cql;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolOptions;

public class CqlClientFactoryTestConstants {

    public static final String HOSTS = "localhost,remotehost:1234";
    public static final String CLUSTER_NAME = "Test Cluster";
    public static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_QUORUM;
    public static final ConsistencyLevel DEFAULT_SERIAL_CONSISTENCY_LEVEL = ConsistencyLevel.SERIAL;
    public static final ProtocolOptions.Compression COMPRESSION = ProtocolOptions.Compression.LZ4;
    public static final String READ_TIMEOUT = "10000";
    public static final String CONNECT_TIMEOUT = "2000";
    public static final String DATA_CENTER_NAME = "philly";

}