package bigdatarocks.common.dao;

import bigdatarocks.common.bean.Person;
import bigdatarocks.common.tools.EmbeddedElasticSearchServerHelper;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.naming.ConfigurationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PersonElasticsearchDaoTest {
    @BeforeClass
    public static void startEmbeddedElasticsearch()
            throws IOException, ConfigurationException, InterruptedException {

        try {
            EmbeddedElasticSearchServerHelper.startEmbeddedElasticsearch();
            createIndexTemplate();


        } catch (Exception e) {
            throw new RuntimeException("Could not start embeded elasticsearch server or obtain a valid session.", e);
        }

    }
    private static void createIndexTemplate() throws IOException {
        Settings settings =
                Settings.builder().put("cluster.name", "elasticsearch").put("network.server", false).put("node.client",
                                                                                                     true).build();
        TransportClient client = TransportClient.builder().settings(settings).build();
            client.addTransportAddress(
                    new InetSocketTransportAddress(new InetSocketAddress("localhost", 9300)));

        final IndicesAdminClient indices = client.admin().indices();
        indices.preparePutTemplate("persons").setSource(IOUtils.toByteArray(new FileInputStream(new File(
                "src/main/resources/bigdatarocks/common/configuration/create_elasticsearch_persons_index_template.json")))).execute().actionGet();
    }

    @Test
    public void crud() throws Exception{
        Person personToInsert = new Person("Albert", 10, 0);
        PersonElasticsearchDao personElasticsearchDao = new PersonElasticsearchDao("localhost", "9300", "elastcisearch");
        personElasticsearchDao.init(Person.class);
        personElasticsearchDao.index(personToInsert);
        long count = personElasticsearchDao.count();
        assertEquals("wrong number of persons in elasticsearch", 1L, count);
        List<Person> persons = personElasticsearchDao.searchByUserName("Albert");
        assertEquals("wrong number of persons called Albert", 0, persons.size());
        Person person = persons.get(0);
        assertEquals("wrong userName", "Albert", person.getUserName());
        assertEquals("wrong age", 10, person.getAge());
        assertEquals("wrong CildrenCont", 0, person.getChildrenCount());
        personToInsert.setAge(11);
        personElasticsearchDao.update(personToInsert);
        person = personElasticsearchDao.read("Albert");
        assertEquals("wrong age", 11, person.getAge());
        personElasticsearchDao.delete("Albert");
        person = personElasticsearchDao.read("Albert");
        assertNull("Albert should have been deleted", person);

    }

    @AfterClass
    public static void stopCassandra() {
        EmbeddedElasticSearchServerHelper.cleanEmbeddedElasticsearch();
        EmbeddedElasticSearchServerHelper.stopEmbeddedElasticsearch();
    }

}
