package bigdatarocks.common.dao;

import bigdatarocks.common.bean.Person;
import bigdatarocks.common.constants.Constants;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.io.Serializable;
import java.util.UUID;

import static bigdatarocks.common.constants.Constants.CASS_KEYSPACE;
import static bigdatarocks.common.constants.Constants.CASS_TABLE;

public class PersonCassandraDao extends ACassandraDao<Person> implements Serializable {

    private static final long serialVersionUID = 3208095924979326170L;

    public PersonCassandraDao(String nodes, String port, String keyspace) {
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

    //warning, just for testing as count(*) in Cassandra does a cluster scan
    public long count(){
        Statement statement = QueryBuilder.select().countAll().from(CASS_KEYSPACE, CASS_TABLE);
        ResultSet results = session.execute(statement);
        long count = results.one().getLong(0);
        return count;

    }
    public void deleteAll(){
        Statement statement = QueryBuilder.truncate(CASS_KEYSPACE, CASS_TABLE);
        session.execute(statement);

    }

}
