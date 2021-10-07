package ru.ainur;

import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

@Slf4j
class ProcessRunner implements Runnable {
    private final ConcurrentHashMap<String, Double> ratesQueue;
    private final PriceProcessor processor;
    private volatile boolean cancel;
    private final Semaphore newDataSemaphore;

    ProcessRunner(PriceProcessor processor) {
        this.processor = processor;
        this.ratesQueue = new ConcurrentHashMap<>(new LinkedHashMap<>());
        this.newDataSemaphore = new Semaphore(0);
    }

    public void addRate(String ccyPair, double rate) {
        log.info("Add rate {} = {}", ccyPair, rate);
        this.ratesQueue.put(ccyPair, rate);
        newDataSemaphore.release();
    }

    public void cancel() {
        this.cancel = true;
    }

    @Override
    public void run() {
        while (!this.cancel) {
            acquire();
            log.debug("Process queue");
            ratesQueue.forEachKey(Long.MAX_VALUE,
                    ccyPair -> {
                        double rate = ratesQueue.remove(ccyPair);
                        log.debug("Send to process rate {} = {}", ccyPair, rate);
                        processor.onPrice(ccyPair, rate);
                    });
            log.debug("End Process queue");
        }
    }

    private void acquire() {
        try {
            newDataSemaphore.acquire();
        } catch (InterruptedException e) {
            log.error("Can't acquire semaphore", e);
        }
    }
}
