package bigdatarocks.common.service;

import bigdatarocks.common.bean.Person;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static junit.framework.Assert.assertEquals;

public class readerServiceTest {
@Test
  public void readPersons() throws IOException{
    List<Person> persons = readerService.readPersons("src/main/resources/common/input/persons.json");
    assertEquals(12, persons.size());
    assertEquals("Etienne", persons.get(0).getName());
    assertEquals(32, persons.get(0).getAge());
    assertEquals(1, persons.get(0).getChildrenCount());

}
}
