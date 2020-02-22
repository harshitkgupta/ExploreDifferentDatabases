import java.util.Map;

import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "graph", name = "vertex",
readConsistency = "QUORUM",
writeConsistency = "QUORUM",
caseSensitiveKeyspace = false,
caseSensitiveTable = false)
public class Vertex {
	@PartitionKey
	String id;
	String type;

	String name;
	@Frozen
	Map<String, Object> attributeMap;
	
	public Vertex(String id, String type, String name,
			Map<String, Object> attributeMap) {
		super();
		this.id = id;
		this.type = type;

		this.name = name;
		this.attributeMap = attributeMap;
	}
}
