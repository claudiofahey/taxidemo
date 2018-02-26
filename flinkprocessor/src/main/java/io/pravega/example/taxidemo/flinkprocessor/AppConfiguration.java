package io.pravega.example.taxidemo.flinkprocessor;

import io.pravega.connectors.flink.util.FlinkPravegaParams;
import io.pravega.connectors.flink.util.StreamId;

import java.io.Serializable;

public class AppConfiguration {

    private final PravegaArgs pravegaArgs = new PravegaArgs();
    private final ElasticSearch elasticSearch = new ElasticSearch();
    private FlinkPravegaParams flinkPravegaParams;
    private String runMode;
    private int parallelism;
    private long checkpointInterval;
    private boolean disableCheckpoint;
    private boolean disableOperatorChaining;
    private boolean enableRebalance;

    public static final String RUN_MODE_AGGREGATE = "aggregate";

    public PravegaArgs getPravegaArgs() {
        return pravegaArgs;
    }

    public ElasticSearch getElasticSearch() {
        return elasticSearch;
    }

    public FlinkPravegaParams getFlinkPravegaParams() {
        return flinkPravegaParams;
    }

    public void setFlinkPravegaParams(FlinkPravegaParams flinkPravegaParams) {
        this.flinkPravegaParams = flinkPravegaParams;
    }

    public String getRunMode() {
        return runMode;
    }

    public void setRunMode(String runMode) {
        this.runMode = runMode;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    public void setCheckpointInterval(long checkpointInterval) {
        this.checkpointInterval = checkpointInterval;
    }

    public boolean isDisableCheckpoint() {
        return disableCheckpoint;
    }

    public void setDisableCheckpoint(boolean disableCheckpoint) {
        this.disableCheckpoint = disableCheckpoint;
    }

    public boolean isDisableOperatorChaining() {
        return disableOperatorChaining;
    }

    public void setDisableOperatorChaining(boolean disableOperatorChaining) {
        this.disableOperatorChaining = disableOperatorChaining;
    }

    public boolean isEnableRebalance() {
        return enableRebalance;
    }

    public void setEnableRebalance(boolean enableRebalance) {
        this.enableRebalance = enableRebalance;
    }

    public static class PravegaArgs {
        protected StreamId inputStream;
        protected int targetRate;
        protected int scaleFactor;
        protected int minNumSegments;
    }

    public static class ElasticSearch implements Serializable {
        private boolean sinkResults;
        private String host;
        private int port;
        private String cluster;
        private String index;
        private String type;

        public boolean isSinkResults() {
            return sinkResults;
        }

        public void setSinkResults(boolean sinkResults) {
            this.sinkResults = sinkResults;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getCluster() {
            return cluster;
        }

        public void setCluster(String cluster) {
            this.cluster = cluster;
        }

        public String getIndex() {
            return index;
        }

        public void setIndex(String index) {
            this.index = index;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

}
