package bigdatarocks.common.dao;

import bigdatarocks.common.bean.Person;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.naming.ConfigurationException;
import java.io.IOException;

import static bigdatarocks.common.constants.Constants.CASS_KEYSPACE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by ashe on 06/05/16.
 */
public class PersonDaoTest {
    private static Session session;
    private static Cluster cluster;

    @BeforeClass
    public static void startEmbeddedCassandra()
            throws IOException, ConfigurationException, InterruptedException {

        try {

            EmbeddedCassandraServerHelper.startEmbeddedCassandra(30000L);
            cluster = new Cluster.Builder().addContactPoint("localhost").withPort(9142).build();
            session = cluster.connect();

            CQLDataLoader dataLoader = new CQLDataLoader(session);
            dataLoader.load(new org.cassandraunit.dataset.cql.FileCQLDataSet("src/main/resources/bigdatarocks/common/configuration/create_cassandra_keyspace.cql"));
        } catch (Exception e) {
            throw new RuntimeException("Could not start embeded cassandra server or obtain a valid session.", e);
        }

    }

    @Test
    public void crud(){
        Person personToInsert = new Person("Albert", 10, 0);
        PersonDao personDao = new PersonDao("localhost", "9142", CASS_KEYSPACE);
        personDao.init(Person.class);
        personDao.create(personToInsert);
        long count = personDao.count();
        assertEquals("wrong number of persons in database", 1L, count);
        Person person = personDao.read("Albert");
        assertEquals("wrong userName", "Albert", person.getUserName());
        assertEquals("wrong age", 10, person.getAge());
        assertEquals("wrong CildrenCont", 0, person.getChildrenCount());
        personToInsert.setAge(11);
        personDao.update(personToInsert);
        person = personDao.read("Albert");
        assertEquals("wrong age", 11, person.getAge());
        personDao.delete("Albert");
        person = personDao.read("Albert");
        assertNull("Albert should have been deleted", person);

    }

    @AfterClass
    public static void stopCassandra() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
    }

}
