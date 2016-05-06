package bigdatarocks.common.dao;

import bigdatarocks.common.bean.Person;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by ashe on 06/05/16.
 */
public class PersonDao extends ACassandraDao<Person> implements Serializable {
    public PersonDao(String nodes, String port, String keyspace) {
        super(nodes, port, keyspace);
    }

    public void create(Person person){
        mapper.save(person);
    }
    public Person read(String userName) {
        return mapper.get(userName);
    }
    public void update(Person person) {
        //primary key is the same cassandra will update entry
        mapper.save(person);
    }
    public void delete(String userName) {
        mapper.delete(userName);
    }

}
