package service;

import bigdatarocks.common.bean.Person;
import bigdatarocks.importer.service.ReaderService;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

public class ReaderServiceTest {
@Test
  public void readPersons() throws IOException{
    List<Person> persons = ReaderService.readPersons("src/main/resources/input/persons.json");
    assertNotNull(persons.get(0).getId());
    assertEquals(12, persons.size());
    assertEquals("Etienne", persons.get(0).getName());
    assertEquals(32, persons.get(0).getAge());
    assertEquals(1, persons.get(0).getChildrenCount());

}
}
