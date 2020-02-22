import org.junit.Test;


public class Test11 extends CommonTest{
	@Test
	public void test(){
		//getJdbcTemplate().queryForList("SELECT * FROM INFORMATION_SCHEMA.SYSTEM_TABLES where TABLE_TYPE='TABLE'",Integer.class);
	getJdbcTemplate().execute("CREATE TABLE IF NOT EXISTS NUMERIC_TABLE "
		+"(ID INTEGER IDENTITY  PRIMARY KEY,"
		+" NUM NUMERIC(38,20) NOT NULL," 
		+"STR VARCHAR(512) NOT NULL)");
	}
}
