import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;


public class CassendraTest {
	CassandraConnector cassandraConnector;
	KeyspaceRepository schemaRepository;
	Session session;
	
	long start;
	
	final String KEYSPACE_NAME = "test";
	@Before
	public void connect() {
		cassandraConnector = new CassandraConnector();
		cassandraConnector.connect("127.0.0.1",null);

		session = cassandraConnector.getSession();
		
        schemaRepository = new KeyspaceRepository(session);
        schemaRepository.createKeyspace(KEYSPACE_NAME, "SimpleStrategy", 0);
        schemaRepository.useKeyspace(KEYSPACE_NAME);
		start= System.currentTimeMillis();
	}
	
	@Test
	public void testTableInsert(){
		session.execute("INSERT INTO users (lastname, age, city, email, firstname) VALUES ('Jones', 35, 'Austin', 'bob@example.com', 'Bob')");
		System.out.println("Insert time : "+(System.currentTimeMillis() - start));
	}
	
}
