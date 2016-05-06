package bigdatarocks.common.dao;

import bigdatarocks.common.bean.Person;
import org.junit.Test;

import static bigdatarocks.common.constants.Constants.CASS_KEYSPACE;

/**
 * Created by ashe on 06/05/16.
 */
public class PersonDaoTest {
    @Test
    public void crud(){
        Person person = new Person("Albert", 10, 0);
        PersonDao personDao = new PersonDao("localhost", "9042", CASS_KEYSPACE);
        personDao.init(Person.class);
        personDao.create(person);
        Person personRead = personDao.read("Albert");

    }
}
