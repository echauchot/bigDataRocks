package bigdatarocks.common.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetSocketAddress;

public abstract class AElasticsearchDao<T> {
    private static final Logger LOGGER = Logger.getLogger(AElasticsearchDao.class);

    protected Client client;
    protected ObjectMapper objectMapper;
    private String nodes;
    private String port;
    private String clusterName;


    public AElasticsearchDao(String nodes, String port, String clusterName) {
        this.nodes = nodes;
        this.port = port;
        this.clusterName = clusterName;
    }

    public void init(Class<T> tClass) {
        try {
            objectMapper = new ObjectMapper();
            if (client == null) {
                Settings settings =
                        Settings.builder().put("cluster.name", clusterName).build();
                TransportClient transClient = TransportClient.builder().settings(settings).build();
                String[] nodeList = nodes.split(",");
                for (String node : nodeList) {
                    transClient.addTransportAddress(
                            new InetSocketTransportAddress(new InetSocketAddress(node, Integer.parseInt(port))));
                    LOGGER.info("Added Elasticsearch node : " + node + ":" + port);
                }
                client = transClient;
            }

        } catch (Exception e) {
            LOGGER.error("Error initializing ElasticsearchDao");
            throw e;
        }
    }

    public synchronized void close() {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                LOGGER.error("Error closing ElasticsearchDao");
                throw e;
            }
        }
    }

    public void setClient(Client client) {
        this.client = client;
    }
}
