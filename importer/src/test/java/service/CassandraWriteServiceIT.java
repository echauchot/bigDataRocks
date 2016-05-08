package service;

import bigdatarocks.common.bean.Person;
import bigdatarocks.common.dao.PersonDao;
import bigdatarocks.importer.pipeline.WritePipeline;
import org.junit.Test;

import java.io.IOException;

import static bigdatarocks.common.constants.Constants.CASS_KEYSPACE;
import static org.junit.Assert.assertEquals;

public class CassandraWriteServiceIT {

    @Test
    public void percistToCassandra() throws IOException {
        WritePipeline pipeliine = new WritePipeline();
        pipeliine.run("src/main/resources/input/persons.json", true, false);
        PersonDao personDao = new PersonDao("localhost", "9042", CASS_KEYSPACE);
        personDao.init(Person.class);
        long count = personDao.count();
        assertEquals("wrong number of persons inserted", 12, count);
        Person etienne = personDao.read("Etienne");
        assertEquals("wrong age for Etienne", 32, etienne.getAge());
        assertEquals("wrong childrenCount for Etienne", 1, etienne.getChildrenCount());


    }
}
