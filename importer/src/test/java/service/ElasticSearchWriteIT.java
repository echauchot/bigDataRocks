package service;

import bigdatarocks.common.bean.Person;
import bigdatarocks.common.dao.PersonElasticsearchDao;
import bigdatarocks.common.tools.ConfigLoader;
import bigdatarocks.importer.pipeline.WritePipeline;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class ElasticSearchWriteIT {

    private static WritePipeline pipeline;
    private static PersonElasticsearchDao personElasticsearchDao;

    @BeforeClass
    public static void initTest() throws Exception {
        Properties properties = ConfigLoader.loadProperties();
        pipeline = new WritePipeline(properties);
        personElasticsearchDao = new PersonElasticsearchDao(properties.getProperty("elasticsearch.nodes"),
                                                            properties.getProperty("elasticsearch.port.transport"),
                                                            properties.getProperty("elasticsearch.clustername"));
        personElasticsearchDao.init(Person.class);
    }

    @Test
    public void percistToElasticSearch() throws IOException {
        pipeline.run("src/main/resources/input/persons.json", false, true);
        long count = personElasticsearchDao.count();
        assertEquals("wrong number of persons in elasticsearch", 12L, count);
        Person person = personElasticsearchDao.read("Etienne");
        assertEquals("wrong userName", "Etienne", person.getUserName());
        assertEquals("wrong age", 32, person.getAge());
        assertEquals("wrong CildrenCont", 1, person.getChildrenCount());
    }

    @AfterClass
    public static void clean() {
        personElasticsearchDao.deleteAll();
    }
}
