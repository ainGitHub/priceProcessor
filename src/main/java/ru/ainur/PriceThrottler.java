package ru.ainur;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class PriceThrottler implements PriceProcessor {
    private final ConcurrentHashMap<PriceProcessor, ProcessRunner> processors;
    private final ExecutorService processExecutor;

    public PriceThrottler() {
        this.processors = new ConcurrentHashMap<>();
        //Can configure that param, or inject ExecutorService
        this.processExecutor = Executors.newFixedThreadPool(10);
    }

    @Override
    public void onPrice(String ccyPair, double rate) {
        log.debug("OnPrice {} {}", ccyPair, rate);
        this.processors.forEachValue(1, p -> p.addRate(ccyPair, rate));
    }

    @Override
    public synchronized void subscribe(PriceProcessor priceProcessor) {
        ProcessRunner processRunner = new ProcessRunner(priceProcessor);
        processors.put(priceProcessor, processRunner);
        processExecutor.submit(processRunner);
    }

    @Override
    public synchronized void unsubscribe(PriceProcessor priceProcessor) {
        processors.remove(priceProcessor).cancel();
    }

    public long getProcessorsCount() {
        return processors.mappingCount();
    }
}
