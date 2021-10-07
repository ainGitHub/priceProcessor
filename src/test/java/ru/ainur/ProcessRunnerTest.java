package ru.ainur;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.*;

@Slf4j
public class ProcessRunnerTest {

    private final Random random;

    public ProcessRunnerTest() {
        this.random = new Random();
    }

    @Test
    public void runOnlyOneTypeRates() {
        PriceProcessor slowProcessor = createWaitProcessorMock(150L);

        ProcessRunner processRunner = new ProcessRunner(slowProcessor);
        CompletableFuture.runAsync(processRunner);

        String ccyPair = "EURRUB";
        Double[] rates = new Double[]{10.0, 15.0, 20.0};
        CompletableFuture<Void> producerFuture = CompletableFuture.runAsync(() ->
                Arrays.stream(rates).forEach(rate -> {
                    try {
                        processRunner.addRate(ccyPair, rate); //Add rate
                        Thread.sleep(120);
                        //Add random rates
                        random.doubles(4).forEach(r -> processRunner.addRate(ccyPair, r));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }));

        producerFuture.join();
        processRunner.cancel();

        verifyCalled(slowProcessor, rates.length, ccyPair, rates);
        //Only executed first, middle and last
    }

    @Test
    public void fastProcessorTest() {
        int ratesCount = 20;
        String ccyPair = "EURRUB";

        PriceProcessor processor = createWaitProcessorMock(null);
        Double[] rates = random.doubles(ratesCount).boxed().toArray(Double[]::new);

        ProcessRunner processRunner = new ProcessRunner(processor);
        CompletableFuture.runAsync(processRunner);

        //send every 5 milliseconds
        Arrays.stream(rates).forEach(rate -> waitedAdd(ccyPair, rate, 10L, processRunner));
        processRunner.cancel();

        verifyCalled(processor, ratesCount, ccyPair, rates);
    }

    @Test
    public void multipleTypeRatesTest() {
        int ratesCount = 20;
        String eurRubPair = "EURRUB";
        String usdRubPair = "USDRUB";

        PriceProcessor processor = createWaitProcessorMock(null);
        Map<String, Double[]> ratesValuesMap = Map.of(
                eurRubPair, random.doubles(ratesCount).boxed().toArray(Double[]::new),
                usdRubPair, random.doubles(ratesCount).boxed().toArray(Double[]::new)
        );

        ProcessRunner processRunner = new ProcessRunner(processor);
        CompletableFuture.runAsync(processRunner);

        List<CompletableFuture<Void>> producers = producers(ratesValuesMap, processRunner);
        CompletableFuture.allOf(producers.toArray(new CompletableFuture[0])).join();
        processRunner.cancel();

        ratesValuesMap.forEach((key, value) -> verifyCalled(processor, value.length, key, value));
    }

    private List<CompletableFuture<Void>> producers(Map<String, Double[]> ratesValuesMap, ProcessRunner runner) {
        return ratesValuesMap.entrySet().stream().map(entry -> CompletableFuture.runAsync(() -> {
            //send every 15 milliseconds
            Arrays.stream(entry.getValue())
                    .forEach(rate -> waitedAdd(entry.getKey(), rate, 15L, runner));
        })).collect(Collectors.toList());
    }

    private void verifyCalled(PriceProcessor processor, int ratesCount, String pair, Double[] values) {
        ArgumentCaptor<Double> usdRubCallCaptor = ArgumentCaptor.forClass(Double.class);
        verify(processor, atLeast(ratesCount)).onPrice(eq(pair), usdRubCallCaptor.capture());
        assertArrayEquals(values, usdRubCallCaptor.getAllValues().toArray(Double[]::new));
    }

    private void waitedAdd(String ccyPair, double rate, Long wait, ProcessRunner runner) {
        try {
            if (wait != null) {
                Thread.sleep(wait);
            }
            runner.addRate(ccyPair, rate);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private PriceProcessor createWaitProcessorMock(Long time) {
        PriceProcessor processor = mock(PriceProcessor.class);
        doAnswer(arg -> {
            log.debug("Start to process rate {} = {}", arg.getArgument(0), arg.getArgument(1));
            if (time != null) {
                Thread.sleep(time);
            }
            log.debug("End to process");
            return null;
        }).when(processor).onPrice(any(String.class), anyDouble());
        return processor;
    }
}