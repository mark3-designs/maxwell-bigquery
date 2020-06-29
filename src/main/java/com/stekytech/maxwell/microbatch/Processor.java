package com.stekytech.maxwell.microbatch;

import com.stekytech.maxwell.CDCEvent;

import java.util.List;

public class Processor implements Runnable {

    public final int batchSize;

    private final Collector collector;
    private boolean shutdown = false;

    public Processor(Collector collector, int batchSize) {
        this.collector = collector;
        this.batchSize = batchSize;
    }

    @Override
    public void run() {

        while (!shutdown) {

            List<CDCEvent> records = collector.nextBatch(batchSize);

            for (CDCEvent event : records) {


                System.out.println(event);


                event.commit();
            }


        }

    }


    public void shutdown() {
        shutdown = true;
    }
}
