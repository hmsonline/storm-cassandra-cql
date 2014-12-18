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
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryOptions;
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
    private String clusterName = null;
    private ConsistencyLevel consistencyLevel= null;
    private ConsistencyLevel serialConsistencyLevel = null;

    protected static Cluster cluster;

    public CqlClientFactory(String hosts) {
        this(hosts, null, ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM);
    }

    public CqlClientFactory(String hosts, ConsistencyLevel clusterConsistency) {
        this(hosts, null, clusterConsistency, ConsistencyLevel.QUORUM);
    }
    
    public CqlClientFactory(String hosts, String clusterName, ConsistencyLevel clusterConsistency, 
            ConsistencyLevel conditionalUpdateConsistency) {
        this.hosts = hosts.split(",");
        this.consistencyLevel = clusterConsistency;
        if (conditionalUpdateConsistency != null){
            this.serialConsistencyLevel = conditionalUpdateConsistency;
        }
        if (clusterName != null) {
            this.clusterName = clusterName;
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
                List<InetSocketAddress> sockets = new ArrayList<InetSocketAddress>();
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

                Cluster.Builder builder = Cluster.builder().addContactPointsWithPorts(sockets);
                QueryOptions queryOptions = new QueryOptions();
                queryOptions.setConsistencyLevel(consistencyLevel);
                queryOptions.setSerialConsistencyLevel(serialConsistencyLevel);
                builder = builder.withQueryOptions(queryOptions);

                if (StringUtils.isNotEmpty(clusterName)) {
                    builder = builder.withClusterName(clusterName);
                }

                cluster = builder.build();
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
