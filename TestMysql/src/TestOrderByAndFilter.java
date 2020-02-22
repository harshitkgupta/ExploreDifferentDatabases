import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;



import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.jdbc.core.JdbcTemplate;



public class TestOrderByAndFilter extends CommonTest{
	
	private static int N =100;
	private long startTime=0,endTime=0,t;
	
	@Rule 
	public TestName name = new TestName();
	
	@Before
	public void createTable(){
//		getJdbcTemplate().execute("CREATE TABLE VISITOR ("
//				+ "ID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,"
//				+ "NAME VARCHAR(40) NOT NULL,"
//				//+ "LOGIN TIMESTAMP DEFAULT CURRENT_TIMESTAMP "
//				+")");
		getJdbcTemplate().execute("CREATE TABLE VISITOR2 ("
				+ "ID varchar(36) not null PRIMARY KEY,"
				+ "NAME VARCHAR(40) NOT NULL,"
				//+ "LOGIN TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
				+" )");
	}
	
	

	@Before
	public void beforeTest(){
		startTime = System.currentTimeMillis();
	}
	
	@After
	public void afterTest(){
		endTime = System.currentTimeMillis();
		System.out.println(name.getMethodName()+" took milisec : "+ (endTime-startTime));
	}
	
	@Test
	public  void test1_insertRecord(){
		for(int i=0;i<N;i++)
		{
		int n= getJdbcTemplate().update("INSERT INTO VISITOR ( NAME ) VALUES (?) ",new Object[]{i,"visitor"+i});
		
		}System.out.println("count of visitor inserted "+N);
	}
	@Test
	public  void test2_insertRecord(){
		for(int i=0;i<N;i++)
		{
		int n= getJdbcTemplate().update("INSERT INTO VISITOR ( NAME ) VALUES (?) ",new Object[]{UUID.randomUUID().toString(),"visitor"+i});
		
		}System.out.println("count of visitor inserted "+N);
	}
}
