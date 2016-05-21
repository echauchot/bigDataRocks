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
import java.util.Properties;

public class WritePipeline {

    private JavaSparkContext sparkContext;

    public WritePipeline(Properties properties) {
        configureSparkContext(properties);
    }

    public void run(String fileName, boolean percistToCassandra, boolean percistToElasticSearch) throws IOException {
        List<Person> persons = InputReadService.readPersons(fileName);
        JavaRDD<Person> personsRdd = sparkContext.parallelize(persons);
        personsRdd.persist(StorageLevel.MEMORY_AND_DISK());
        if (percistToCassandra)
            CassandraWriteService.percistToCassandra(personsRdd);
        if (percistToElasticSearch)
            ElasticSearchWriteService.percistToElasticSearch(personsRdd);

    }

    private void configureSparkContext(Properties properties) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Write pipeline");
        sparkConf.set("spark.driver.allowMultipleContexts", "true");

        sparkConf.setMaster(properties.getProperty("spark.master"));
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        sparkConf.set("spark.cassandra.connection.host", properties.getProperty("cassandra.nodes"));
        sparkConf.set("spark.cassandra.output.batch.size.bytes", properties.getProperty("cassandra.batch.size.bytes"));
        sparkConf.set("spark.cassandra.connection.port", properties.getProperty("cassandra.port"));

        sparkConf.set("es.nodes", properties.getProperty("elasticsearch.nodes") + ":" + properties.getProperty("elasticsearch.port.rest"));
        sparkConf.set("es.batch.size.entries", properties.getProperty("elasticsearch.batch.size.entries"));
        sparkConf.set("es.batch.size.bytes", properties.getProperty("elasticsearch.batch.size.bytes"));
        sparkConf.set("es.nodes.discovery", properties.getProperty("elasticsearch.nodes.dicovery"));

        sparkContext = new JavaSparkContext(sparkConf);
    }
}
