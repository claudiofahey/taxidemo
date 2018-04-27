package io.pravega.example.taxidemo.flinkprocessor;

public class RawDataAggregator {
    public static class Result {
        public long timestamp;
        public double tip_amount;
        public double total_amount;

        @Override
        public String toString() {
            return "Result{" +
                    "timestamp=" + timestamp +
                    ", tip_amount=" + tip_amount +
                    ", total_amount=" + total_amount +
                    '}';
        }
    }

    private static class Accumulator {
        public Result lastAdded;       // Cummulative totals when add() was last called.
        public Result lastEmitted;     // Cummulative totals when getResult() was last called.

        public Accumulator() {
            lastAdded = new Result();
            lastEmitted = new Result();
        }

        @Override
        public String toString() {
            return "Accumulator{" +
                    "lastAdded=" + lastAdded +
                    ", lastEmitted=" + lastEmitted +
                    '}';
        }
    }

    public static class AggregateFunction
            implements org.apache.flink.api.common.functions.AggregateFunction<RawData, Accumulator, Result> {

        @Override
        public Accumulator createAccumulator() {
            return new Accumulator();
        }

        // add is called on the window's accumulator after each new RawData value.
        @Override
        public Accumulator add(RawData value, Accumulator acc) {
            acc.lastAdded.timestamp = value.timestamp;
            acc.lastAdded.tip_amount = value.tip_amount;
            acc.lastAdded.total_amount = value.total_amount;
            return acc;
        }

        // getResult is called whenever the window is triggered.
        // It returns the delta tip_amount and total_amount since the last call.
        @Override
        public Result getResult(Accumulator acc) {
            Result result = new Result();
            result.timestamp = acc.lastAdded.timestamp;
            result.tip_amount = acc.lastAdded.tip_amount - acc.lastEmitted.tip_amount;
            result.total_amount = acc.lastAdded.total_amount - acc.lastEmitted.total_amount;
            acc.lastEmitted.timestamp = acc.lastAdded.timestamp;
            acc.lastEmitted.tip_amount = acc.lastAdded.tip_amount;
            acc.lastEmitted.total_amount = acc.lastAdded.total_amount;
            return result;
        }

        @Override
        public Accumulator merge(Accumulator a, Accumulator b) {
            throw new UnsupportedOperationException();
        }
    }
}
