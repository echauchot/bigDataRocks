package bigdatarocks.common.tools;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import java.io.File;
import java.io.IOException;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class EmbeddedElasticSearchServerHelper {
    private static final String DEFAULT_DATA_DIRECTORY = "target/elasticsearch-data";

    private static Node node;


    public static void startEmbeddedElasticsearch() {

        Settings.Builder elasticsearchSettings =
                Settings.settingsBuilder().put("http.enabled", "true").put("path.data", DEFAULT_DATA_DIRECTORY).put("path.home", DEFAULT_DATA_DIRECTORY).put(
                        "script.engine.groovy.inline.update", "on");

        node = nodeBuilder().local(true).settings(elasticsearchSettings.build()).node();
    }

    public static Client getClient() {
        return node.client();
    }

    public static void stopEmbeddedElasticsearch() {
        node.close();
    }

    public static void cleanEmbeddedElasticsearch() {
        try {
            FileUtils.deleteDirectory(new File(DEFAULT_DATA_DIRECTORY));
        } catch (IOException e) {
            throw new RuntimeException("Could not delete data directory of embedded elasticsearch server", e);
        }
    }
}
