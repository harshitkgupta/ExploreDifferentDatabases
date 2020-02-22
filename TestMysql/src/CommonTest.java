import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;


public class CommonTest {
	private static ApplicationContext context = new ClassPathXmlApplicationContext("resource/Spring-Beans.xml");
	private static JdbcTemplate jdbcTemplate = context.getBean("jdbcTemplate", JdbcTemplate.class);
	public static JdbcTemplate getJdbcTemplate(){
		return jdbcTemplate;
	}
}
