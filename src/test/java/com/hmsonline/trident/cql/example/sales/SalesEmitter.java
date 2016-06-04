package com.hmsonline.trident.cql.example.sales;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout.Emitter;
import org.apache.storm.trident.topology.TransactionAttempt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class SalesEmitter implements Emitter<Long>, Serializable {
    private static final long serialVersionUID = 1L;
    public static AtomicInteger successfulTransactions = new AtomicInteger(0);
    public static AtomicInteger uids = new AtomicInteger(0);
    private Random generator = new Random();
    private String[] states = {"DE", "MD", "PA", "NJ", "NY"};
    private String[] products = {"lego", "brick", "bike", "horn"};

    @Override
    public void emitBatch(TransactionAttempt tx, Long coordinatorMeta,
                          TridentCollector collector) {
        for (int i = 0; i < 100; i++) {
            List<Object> sale = new ArrayList<Object>();
            sale.add(states[generator.nextInt(4)]);
            sale.add(products[generator.nextInt(4)]);
            sale.add(generator.nextInt(100));
            collector.emit(sale);
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
