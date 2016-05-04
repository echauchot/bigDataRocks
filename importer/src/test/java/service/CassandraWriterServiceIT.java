package service;

import bigdatarocks.importer.pipeline.WritePipeline;
import org.junit.Test;

import java.io.IOException;

public class CassandraWriterServiceIT {

    @Test
    public void percistToCassandra() throws IOException {
        WritePipeline pipeliine = new WritePipeline();
        pipeliine.run("src/main/resources/input/persons.json", true, false);

    }
}
