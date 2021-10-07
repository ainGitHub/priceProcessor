package ru.ainur;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class PriceThrottlerTest {

    @Test
    public void subscribeTest() {
        long processorsCount = 100;
        PriceThrottler priceThrottler = new PriceThrottler();

        LongStream.range(0, processorsCount)
                .mapToObj(EmptyPriceProcessor::new)
                .limit(processorsCount)
                .parallel()
                .forEach(priceThrottler::subscribe);

        assertEquals(processorsCount, priceThrottler.getProcessorsCount());
    }

    @Test
    public void unSubscribeTest() {
        int processorsCount = 10;
        PriceThrottler priceThrottler = new PriceThrottler();

        List<PriceProcessor> processors = IntStream.range(0, processorsCount)
                .limit(processorsCount)
                .mapToObj(EmptyPriceProcessor::new)
                .collect(Collectors.toList());
        processors.forEach(priceThrottler::subscribe);
        assertEquals(processorsCount, priceThrottler.getProcessorsCount());

        processors.stream().parallel().forEach(priceThrottler::unsubscribe);
        assertEquals(0, priceThrottler.getProcessorsCount());
    }

    @Test
    public void onPriceAllRatesExecutedTest() {
        String rateType = "EURRUB";

        PriceProcessor slowProcessor = mock(PriceProcessor.class);
        doAnswer(invocationOnMock -> {
            Thread.sleep(10);
            return null;
        }).when(slowProcessor).onPrice(any(String.class), anyDouble());

        PriceThrottler priceThrottler = new PriceThrottler();
        priceThrottler.subscribe(slowProcessor);

        DoubleStream.of(1, 2, 3, 4, 5)
                .forEach(rate -> {
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    priceThrottler.onPrice(rateType, rate);
                });

        verify(slowProcessor).onPrice(rateType, 1);
        verify(slowProcessor).onPrice(rateType, 2);
        verify(slowProcessor).onPrice(rateType, 3);
        verify(slowProcessor).onPrice(rateType, 4);
        verify(slowProcessor).onPrice(rateType, 5);
    }

    @Test
    public void onPriceMultipleRatesTest() {
        String eurRubType = "EURRUB";
        String usdRubType = "USDRUB";

        PriceProcessor slowProcessor = mock(PriceProcessor.class);

        PriceThrottler priceThrottler = new PriceThrottler();
        priceThrottler.subscribe(slowProcessor);

        List<Double> eurRubRates = Arrays.asList(1.0, 2.0, 3.0);
        eurRubRates.forEach(d -> priceThrottler.onPrice(eurRubType, d));

        List<Double> usdRubRates = Arrays.asList(4.0, 5.0, 6.0);
        usdRubRates.forEach(d -> priceThrottler.onPrice(usdRubType, d));

        ArgumentCaptor<Double> eurRubCaptor = ArgumentCaptor.forClass(Double.class);
        ArgumentCaptor<Double> usdRubCaptor = ArgumentCaptor.forClass(Double.class);
        verify(slowProcessor, atLeast(1))
                .onPrice(eq(eurRubType), eurRubCaptor.capture());
        verify(slowProcessor, atLeast(1))
                .onPrice(eq(usdRubType), usdRubCaptor.capture());

        assertTrue(eurRubRates.containsAll(eurRubCaptor.getAllValues()));
        assertTrue(usdRubRates.containsAll(usdRubCaptor.getAllValues()));
    }

    private static class EmptyPriceProcessor implements PriceProcessor {
        private final long id;

        private EmptyPriceProcessor(long id) {
            this.id = id;
        }

        @Override
        public void onPrice(String ccyPair, double rate) {
        }

        @Override
        public void subscribe(PriceProcessor priceProcessor) {
        }

        @Override
        public void unsubscribe(PriceProcessor priceProcessor) {
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EmptyPriceProcessor that = (EmptyPriceProcessor) o;
            return id == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

}