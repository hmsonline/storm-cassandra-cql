package com.hmsonline.trident.cql.example.simpleupdate;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout.Emitter;
import org.apache.storm.trident.topology.TransactionAttempt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleUpdateEmitter implements Emitter<Long>, Serializable {
    private static final long serialVersionUID = 1L;
    public static AtomicInteger successfulTransactions = new AtomicInteger(0);
    public static AtomicInteger uids = new AtomicInteger(0);

    @Override
    public void emitBatch(TransactionAttempt tx, Long coordinatorMeta,
                          TridentCollector collector) {
        for (int i = 0; i < 100; i++) {
            List<Object> message = new ArrayList<Object>();
            message.add(Integer.toString(i));
            collector.emit(message);
        }
    }

    @Override
    public void success(TransactionAttempt tx) {
        successfulTransactions.incrementAndGet();
    }

    @Override
    public void close() {
    }
}
