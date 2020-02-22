import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;








import com.cadence.adw.common.datamodel.ECADRelationInstance;
import com.cadence.adw.common.datamodel.IDatamodel;
import com.cadence.adw.common.generic.xml.datamodels.Parser;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;


public class HPInCassendra {
	public static void main( String[] args )
    {
		CassandraConnector connector = new CassandraConnector();
        connector.connect("127.0.0.1", null);
        Session session = connector.getSession();
        KeyspaceRepository sr = new KeyspaceRepository(session);
        sr.createKeyspace("graph", "SimpleStrategy", 1);
        sr.useKeyspace("graph");
        MappingManager manager = new MappingManager(session);
		
		Mapper<Vertex> mapper = manager.mapper(Vertex.class);
		Parser parser = new Parser();
		boolean isParseSuccess = parser.parse("D:\\ADWSERVER172\\hp\\base.xml");
		boolean withBatchMode = false;
		System.out.println("Batch Mode Status "+(withBatchMode?"ON":"OFF"));
		long start = System.currentTimeMillis();
		if(isParseSuccess){
			HashMap metaDataObjects = parser.getMetaDataObjects();
			HashMap metaDataRelations = parser.getMetaDataRelations();
			HashMap partsDataObjects = parser.getPartsDataObjects();
			HashMap partsDataRelations = parser.getPartsDataRelations();

			try{
			int vertexCount = 0; 
			int i=0;
			HashMap<String,Object> idToVertexMap = new HashMap<String,Object>();
			Set<String> partsDataObjectKeys= partsDataObjects.keySet();
			for(Object key: partsDataObjectKeys){
				String type = (String)key;
				List partObjects = (ArrayList) partsDataObjects.get(type);
				
				for(Object partObject: partObjects){
					IDatamodel dataModelObject = (IDatamodel)partObject;
					
					Vertex v= new Vertex(dataModelObject.getID(),type, dataModelObject.getDisplayName()	, dataModelObject.getAttributesMap());
					
					idToVertexMap.put(dataModelObject.getObjectID(), v);
					
					i++;
					vertexCount++;
					mapper.save(v);
				}
			}

			System.out.println("Vertex Count "+vertexCount);
			System.out.println("Vertex insert took "+(System.currentTimeMillis()-start)+" mili sec");
			

			}catch(Exception e){
				e.printStackTrace(System.out);
			}finally{
				sr.deleteKeyspace("graph");
			}
			
		}
		else{
			System.err.println("Parsing of base.xml has been failed");
		}
			
    }
}