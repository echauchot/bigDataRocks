package bigdatarocks.common.bean;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import static bigdatarocks.common.constants.Constants.CASS_KEYSPACE;
import static bigdatarocks.common.constants.Constants.CASS_TABLE;

@Table(keyspace = CASS_KEYSPACE, name = CASS_TABLE, readConsistency = "ONE")
public class Person {

    @PartitionKey(0)
    @Column(name = "user_name")
    private String userName;

    @Column(name = "age")
    private int age;

    @Column(name = "children_count")
    private int childrenCount;


    public Person(String userName, int age, int childrenCount) {
        this.userName = userName;
        this.age = age;
        this.childrenCount = childrenCount;
    }

    public Person() {
        super();
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getChildrenCount() {
        return childrenCount;
    }

    public void setChildrenCount(int childrenCount) {
        this.childrenCount = childrenCount;
    }
}
