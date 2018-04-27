package io.pravega.example.taxidemo.flinkprocessor;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class StatisticsAggregator {
    public static class Result {
        public long timestamp;
        public double tip_percent;

        @Override
        public String toString() {
            return "Result{" +
                    "timestamp=" + timestamp +
                    ", tip_percent=" + tip_percent +
                    '}';
        }
    }

    private static class Accumulator {
        public long timestamp;
        public double tip_amount;
        public double total_amount;

        @Override
        public String toString() {
            return "Accumulator{" +
                    "timestamp=" + timestamp +
                    ", tip_amount=" + tip_amount +
                    ", total_amount=" + total_amount +
                    '}';
        }
    }

    public static class AggregateFunction
            implements org.apache.flink.api.common.functions.AggregateFunction<RawDataAggregator.Result, Accumulator, Result> {

        @Override
        public Accumulator createAccumulator() {
            return new Accumulator();
        }

        @Override
        public Accumulator add(RawDataAggregator.Result value, Accumulator acc) {
            acc.timestamp = value.timestamp;
            acc.tip_amount += value.tip_amount;
            acc.total_amount += value.total_amount;
            return acc;
        }

        // getResult is called whenever the window is triggered.
        // It returns the delta tip_amount and total_amount since the last call.
        @Override
        public Result getResult(Accumulator acc) {
            Result result = new Result();
            result.timestamp = acc.timestamp;
            result.tip_percent = 100.0 * acc.tip_amount / acc.total_amount;
            return result;
        }

        @Override
        public Accumulator merge(Accumulator a, Accumulator b) {
            throw new UnsupportedOperationException();
        }
    }

    public static class AllWindowFunction
            implements org.apache.flink.streaming.api.functions.windowing.AllWindowFunction<Result, Result, TimeWindow> {

        public void apply(TimeWindow window,
                          Iterable<Result> values,
                          Collector<Result> out) {
            Result result = values.iterator().next();
            result.timestamp = window.getEnd();
            out.collect(result);
        }
    }
}
