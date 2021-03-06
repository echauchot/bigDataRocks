package bigdatarocks.common.dao;

import bigdatarocks.common.bean.Person;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.naming.ConfigurationException;
import java.io.IOException;

import static bigdatarocks.common.constants.Constants.CASS_KEYSPACE;
import static bigdatarocks.common.constants.Constants.CASS_TABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PersonCassandraDaoTest {

    private static Session session;
    private static Cluster cluster;
    private static PersonCassandraDao personCassandraDao;

    @BeforeClass
    public static void initTest() throws Exception {

        //this test will be slow because it needs to start Casssandra embedded server.
        // For the test to be run on slow machines,
        // the embedded cassandra start timeout is set bellow to 32s ! (which is pretty slow for a unit test)
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(32000L);
        cluster = new Cluster.Builder().addContactPoint("localhost").withPort(9142).build();
        session = cluster.connect();
        createKeyspace();
        personCassandraDao = new PersonCassandraDao("localhost", "9142", CASS_KEYSPACE);
        personCassandraDao.init(Person.class);
    }

    private static void createKeyspace() {
        CQLDataLoader dataLoader = new CQLDataLoader(session);
        dataLoader.load(new org.cassandraunit.dataset.cql.FileCQLDataSet(
                "src/main/resources/bigdatarocks/common/configuration/cassandra_keyspace.cql"));
    }

    @Test
    public void crud() {
        Person personToInsert = new Person("Albert", 10, 0);
        personCassandraDao.create(personToInsert);
        long count = personCassandraDao.count();
        assertEquals("wrong number of persons in database", 1L, count);
        Person person = personCassandraDao.read("Albert");
        assertEquals("wrong userName", "Albert", person.getUserName());
        assertEquals("wrong age", 10, person.getAge());
        assertEquals("wrong CildrenCont", 0, person.getChildrenCount());
        personToInsert.setAge(11);
        personCassandraDao.update(personToInsert);
        person = personCassandraDao.read("Albert");
        assertEquals("wrong age", 11, person.getAge());
        personCassandraDao.delete("Albert");
        person = personCassandraDao.read("Albert");
        assertNull("Albert should have been deleted", person);
    }

    @Test
    public void testTruncateTable() {
        personCassandraDao.deleteAll();
        long count = personCassandraDao.count();
        assertEquals("table " + CASS_TABLE + " should be empty", 0, count);
    }

    @AfterClass
    public static void stopCassandra() {
        //Sometimes cleaning request to EmbeddedCassandra timeouts but it is not a big deal not to clean embeddedCassandra
        // because keyspace is dropped and recreated in the initialization of this test. It is better not to break the build
        // on slow machines
        try {
            EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        } catch (OperationTimedOutException e) {
            e.printStackTrace();
        }
    }

}
