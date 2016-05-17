package service;

import bigdatarocks.importer.pipeline.WritePipeline;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ElasticSearchWriteIT {
    @Test
    public void percistToElasticSearch() throws IOException {
        WritePipeline pipeliine = new WritePipeline();
        pipeliine.run("src/main/resources/input/persons.json", false, true);



    }

}
