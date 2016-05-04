package bigdatarocks.importer.service;

import bigdatarocks.common.bean.Person;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import org.apache.spark.api.java.JavaRDD;

import static bigdatarocks.common.constants.Constants.CASS_KEYSPACE;
import static bigdatarocks.common.constants.Constants.CASS_TABLE;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

public class CassandraWriterService {

    public static void percistToCassandra(JavaRDD<Person> personsRdd) {
        CassandraJavaUtil.javaFunctions(personsRdd).writerBuilder(CASS_KEYSPACE, CASS_TABLE,
                                                                  mapToRow(Person.class)).saveToCassandra();
    }

}
