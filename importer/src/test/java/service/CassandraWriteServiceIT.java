package service;

import bigdatarocks.common.bean.Person;
import bigdatarocks.common.dao.PersonCassandraDao;
import bigdatarocks.importer.pipeline.WritePipeline;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static bigdatarocks.common.constants.Constants.CASS_KEYSPACE;
import static org.junit.Assert.assertEquals;

public class CassandraWriteServiceIT {

    private static WritePipeline pipeline;
    private static PersonCassandraDao personCassandraDao;

    @BeforeClass
    public static void initTest() {
        pipeline = new WritePipeline();
        personCassandraDao = new PersonCassandraDao("172.17.0.2", "9042", CASS_KEYSPACE);
        personCassandraDao.init(Person.class);
    }

    @Test
    public void percistToCassandra() throws IOException {
        pipeline.run("src/main/resources/input/persons.json", true, false);
        long count = personCassandraDao.count();
        assertEquals("wrong number of persons inserted", 12L, count);
        Person etienne = personCassandraDao.read("Etienne");
        assertEquals("wrong age for Etienne", 32, etienne.getAge());
        assertEquals("wrong childrenCount for Etienne", 1, etienne.getChildrenCount());
    }

    @AfterClass
    public static void clean(){
        personCassandraDao.deleteAll();
    }
}
