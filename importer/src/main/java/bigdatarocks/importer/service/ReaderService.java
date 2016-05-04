package bigdatarocks.importer.service;

import bigdatarocks.common.bean.Person;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReaderService {

    public static List<Person> readPersons(String fileName) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        File file = new File(fileName);
        ArrayList<Person> persons = mapper.readValue(file, new TypeReference<ArrayList<Person>>() {
        });
        return (persons);
    }
}
