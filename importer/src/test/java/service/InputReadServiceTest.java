package service;

import bigdatarocks.common.bean.Person;
import bigdatarocks.importer.service.InputReadService;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

public class InputReadServiceTest {
@Test
  public void readPersons() throws IOException{
    List<Person> persons = InputReadService.readPersons("src/main/resources/input/persons.json");
    assertEquals(12, persons.size());
    assertEquals("Etienne", persons.get(0).getUserName());
    assertEquals(32, persons.get(0).getAge());
    assertEquals(1, persons.get(0).getChildrenCount());

}
}
