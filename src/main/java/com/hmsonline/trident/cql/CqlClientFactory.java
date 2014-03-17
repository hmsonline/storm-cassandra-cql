package com.hmsonline.trident.cql;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

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
    protected static Cluster cluster;

    @SuppressWarnings("rawtypes")
    public CqlClientFactory(Map configuration) {
        String hostProperty = (String) configuration.get(CassandraCqlStateFactory.TRIDENT_CASSANDRA_CQL_HOSTS);
        hosts = hostProperty.split(";");
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
                if (LOG.isDebugEnabled()) {
                    for (String host : hosts) {
                        LOG.debug("Connecting to [" + host + "]");
                    }
                }
                cluster = Cluster.builder().addContactPoints(hosts).build();
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
