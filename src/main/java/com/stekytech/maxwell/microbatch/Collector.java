package com.stekytech.maxwell.microbatch;

import com.stekytech.maxwell.CDCEvent;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.SynchronousQueue;

public class Collector {

    private Queue<CDCEvent> events = new LinkedList<>();
    private List<Processor> processors = new ArrayList<>();
    private List<Thread> threads = new ArrayList<>();

    public Collector(int threads, int batchSize) {

        for (int i= 0; i < threads; i++) {
            Processor proc = new Processor(this, batchSize);
            Thread thread = new Thread(proc);
            this.processors.add(proc);
            this.threads.add(thread);
            new SynchronousQueue<>();
        }
    }


    public ArrayList<CDCEvent> nextBatch(int size) {
        ArrayList<CDCEvent> batch = new ArrayList<>();
        synchronized (events) {
            while (events.peek() != null && batch.size() < size) {
                batch.add(events.poll());
            }
        }
        return batch;

    }


    public void start() {
        for (Thread thread : threads) {
            thread.start();
        }
    }

    public void stop() {
        for (Processor proc : processors) {
            proc.shutdown();
        }

        for (Thread thread : threads) {
            while (thread.isAlive()) {
                // wait
                try {
                    Thread.sleep(10);
                } catch (InterruptedException interrupted) {

                }
            }
        }
    }

    public void collect(CDCEvent event) {
        events.add(event);
    }




}
