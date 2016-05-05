package bigdatarocks.importer.service;

import bigdatarocks.common.bean.Person;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.JavaRDD;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import static bigdatarocks.common.constants.Constants.ES_INDEX;
import static bigdatarocks.common.constants.Constants.ES_DOCTYPE;

public class ElasticSearchWriteService {

    public static void percistToElasticSearch(JavaRDD<Person> personsRdd) {
        JavaEsSpark.saveToEs(personsRdd, ES_INDEX + "/" + ES_DOCTYPE);
    }

}
