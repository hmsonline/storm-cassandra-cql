package com.hmsonline.trident.cql;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * @author boneill
 */
public class CqlClientFactory implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CqlClientFactory.class);
    private Map<String, Session> sessions = new HashMap<String, Session>();
    private Session defaultSession = null;
    private String[] hosts;
    private String[] ports;
    protected static Cluster cluster;

    @SuppressWarnings("rawtypes")
    public CqlClientFactory(Map configuration) {
        String hostProperty = (String) configuration.get(CassandraCqlStateFactory.TRIDENT_CASSANDRA_CQL_HOSTS);
        hosts = hostProperty.split(",");
        String portProperty = (String) configuration.get(CassandraCqlStateFactory.TRIDENT_CASSANDRA_CQL_PORTS);
        if(StringUtils.isNotBlank(portProperty)) {
            ports = portProperty.split(",");
        }
    }

    public synchronized Session getSession(String keyspace) {
        Session session = sessions.get(keyspace);
        if (session == null) {
            LOG.debug("Constructing session for keyspace [" + keyspace + "]");
            session = getCluster().connect(keyspace);
            sessions.put(keyspace, session);
        }
        return session;
    }

    public synchronized Session getSession() {
        if (defaultSession == null)
            defaultSession = getCluster().connect();
        return defaultSession;
    }
    
    public Cluster getCluster() {
        if (cluster == null) {
            try {
                if(ports != null) {
                List<InetSocketAddress> sockets = new ArrayList<InetSocketAddress>();                    
                int i = 0;
                for (String host : hosts) {
                    int port;
                    if(ports.length == 1) {
                        port = Integer.valueOf(ports[0]);
                    } else {
                        port = Integer.valueOf(ports[i]);
                    }
                    sockets.add(new InetSocketAddress(host, port));
                    LOG.debug("Connecting to [" + host + "] with port [" + port + "]");
                }
                cluster = Cluster.builder().addContactPointsWithPorts(sockets).build();
                } else {
                    if(LOG.isDebugEnabled()) {
                        for (String host : hosts) {
                            LOG.debug("Connecting to [" + host + "]");
                        }
                    }
                    cluster = Cluster.builder().addContactPoints(hosts).build();
                }
                if (cluster == null) {
                    throw new RuntimeException("Critical error: cluster is null after "
                            + "attempting to build with contact points (hosts) " + hosts);
                }
            } catch (NoHostAvailableException e) {
                throw new RuntimeException(e);
            }
        }
        return cluster;
    }
}
