package bigdatarocks.importer.pipeline;

import bigdatarocks.common.bean.Person;
import bigdatarocks.importer.service.CassandraWriteService;
import bigdatarocks.importer.service.ElasticSearchWriteService;
import bigdatarocks.importer.service.InputReadService;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;
import java.util.List;

public class WritePipeline {

    private JavaSparkContext sparkContext;

    public void run(String fileName, boolean percistToCassandra, boolean percistToElasticSearch) throws IOException {
        configureSparkContext();
        //TODO parametrize input
        List<Person> persons = InputReadService.readPersons("src/main/resources/common/input/persons.json");
        JavaRDD<Person> personsRdd = sparkContext.parallelize(persons);
        personsRdd.persist(StorageLevel.MEMORY_AND_DISK());
        if (percistToCassandra)
            CassandraWriteService.percistToCassandra(personsRdd);
        if (percistToElasticSearch)
            ElasticSearchWriteService.percistToElasticSearch(personsRdd);

    }

    private void configureSparkContext() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Write pipeline");
        sparkConf.set("spark.driver.allowMultipleContexts", "true");

        //TODO parameters in config file
        sparkConf.setMaster("local[*]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        sparkConf.set("spark.cassandra.connection.host", "localhost");
        sparkConf.set("spark.cassandra.output.batch.size.bytes", "64192");
        sparkConf.set("spark.cassandra.connection.port", "9042");

        sparkConf.set("es.nodes", "localhost:9200");
        sparkConf.set("es.batch.size.entries", "1000");
        sparkConf.set("es.batch.size.bytes", "2M");
        sparkConf.set("es.nodes.discovery", "true");

        sparkContext = new JavaSparkContext(sparkConf);
    }
}
