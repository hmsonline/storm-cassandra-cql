package com.hmsonline.trident.cql.example.simpleupdate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.trident.spout.ITridentSpout.BatchCoordinator;

import java.io.Serializable;

public class DefaultCoordinator implements BatchCoordinator<Long>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory
            .getLogger(DefaultCoordinator.class);

    @Override
    public boolean isReady(long txid) {
        return true;
    }

    @Override
    public void close() {
    }

    @Override
    public Long initializeTransaction(long txid, Long prevMetadata,
                                      Long currMetadata) {
        LOG.info("Initializing Transaction [" + txid + "]");
        return null;
    }

    @Override
    public void success(long txid) {
        LOG.info("Successful Transaction [" + txid + "]");
    }

}
