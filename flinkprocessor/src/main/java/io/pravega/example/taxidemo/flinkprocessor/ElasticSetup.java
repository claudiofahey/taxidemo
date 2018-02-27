package io.pravega.example.taxidemo.flinkprocessor;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.URL;
import java.util.Collections;
import java.util.Map;

public class ElasticSetup {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSetup.class);

    private static final String VISUALIZATION_ID = "a60144d4-921b-472c-8616-f7d524ddb150";

    private final AppConfiguration.ElasticSearch elasticConfig;

    public ElasticSetup(AppConfiguration.ElasticSearch elasticConfig) {
        this.elasticConfig = elasticConfig;
    }

    public void run() throws Exception {
        String host = elasticConfig.getHost();
        int port = elasticConfig.getPort();
        String cluster = elasticConfig.getCluster();
        Settings settings = Settings.builder()
                .put("cluster.name", cluster)
                .put("client.transport.sniff", "false")
                .build();
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));

        boolean deleteIndex = true;
        if (deleteIndex) {
            LOG.info("Deleting old elastic search index");
            try {
                client.admin().indices().delete(Requests.deleteIndexRequest(elasticConfig.getIndex())).actionGet();
            } catch (IndexNotFoundException e) {
                // Ignore exception.
            }
        }

        LOG.info("Creating elastic Search Index");
        String indexBody = getTemplate("car-event-elastic-index.json", Collections.singletonMap("type", elasticConfig.getType()));
        try {
            client.admin().indices().create(Requests.createIndexRequest(elasticConfig.getIndex()).source(indexBody)).actionGet();
        } catch (ResourceAlreadyExistsException e) {
            // Ignore exception.
        }

        // TODO: Need to export Kibana objects and update related json files.

//        LOG.info("Creating Kibana Index Pattern");
//        String kibanaIndex = getTemplate("car-event-kibana-index-pattern.json", Collections.singletonMap("index", elasticConfig.getIndex()));
//        client.index(Requests.indexRequest(".kibana").type("index-pattern").id(elasticConfig.getIndex()).source(kibanaIndex)).actionGet();
//
//        LOG.info("Creating Kibana Search");
//        String kibanaSearch = getTemplate("car-event-kibana-search.json", Collections.singletonMap("index", elasticConfig.getIndex()));
//        client.index(Requests.indexRequest(".kibana").type("search").id("anomalies").source(kibanaSearch)).actionGet();
//
//        LOG.info("Creating Kibana Visualization");
//        String visualizationBody = getTemplate("car-event-kibana-visualization.json", Collections.singletonMap("index", elasticConfig.getIndex()));
//        client.index(Requests.indexRequest(".kibana").type("visualization").id(VISUALIZATION_ID).source(visualizationBody)).actionGet();
    }

    private String getTemplate(String file, Map<String, String> values) throws Exception {
        URL url = getClass().getClassLoader().getResource(file);
        if (url == null) {
            throw new IllegalStateException("Template file " + file + " not found");
        }

        String body = IOUtils.toString(url.openStream());
        for (Map.Entry<String, String> value : values.entrySet()) {
            body = body.replace("@@" + value.getKey() + "@@", value.getValue());
        }

        return body;
    }
}
