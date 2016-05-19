package bigdatarocks.common.dao;

import bigdatarocks.common.bean.Person;
import bigdatarocks.common.tools.EmbeddedElasticSearchServerHelper;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.naming.ConfigurationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PersonElasticsearchDaoTest {
    private static Client client;
    @BeforeClass
    public static void startEmbeddedElasticsearch()
            throws IOException, ConfigurationException, InterruptedException {

        try {
            EmbeddedElasticSearchServerHelper.startEmbeddedElasticsearch();
            client = EmbeddedElasticSearchServerHelper.getClient();
            createIndexTemplate();


        } catch (Exception e) {
            throw new RuntimeException("Could not start embeded elasticsearch server or obtain a valid session.", e);
        }

    }
    private static void createIndexTemplate() throws IOException {
        final IndicesAdminClient indices = client.admin().indices();
        indices.preparePutTemplate("persons").setSource(IOUtils.toByteArray(new FileInputStream(new File(
                "src/main/resources/bigdatarocks/common/configuration/elasticsearch_persons_index_template.json")))).execute().actionGet();
    }

    @Test
    public void crud() throws Exception{
        Person personToInsert = new Person("Albert", 10, 0);
        PersonElasticsearchDao personElasticsearchDao = new PersonElasticsearchDao(null, null, null);
        personElasticsearchDao.setClient(client);
        personElasticsearchDao.init(Person.class);
        personElasticsearchDao.create(personToInsert);
        //leave elasticsearch a bit of time for indexing new data.
        Thread.sleep(1000);
        long count = personElasticsearchDao.count();
        assertEquals("wrong number of persons in elasticsearch", 1L, count);

        List<Person> persons = personElasticsearchDao.searchByUserName("Albert");
        assertEquals("wrong number of persons called Albert", 1, persons.size());
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
    public static void stopElasticsearch() {
        EmbeddedElasticSearchServerHelper.cleanEmbeddedElasticsearch();
        EmbeddedElasticSearchServerHelper.stopEmbeddedElasticsearch();
    }

}
