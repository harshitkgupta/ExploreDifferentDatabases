

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;


import com.blazegraph.gremlin.embedded.BlazeGraphFactory;
import com.blazegraph.gremlin.structure.BlazeGraph;
import com.cadence.adw.common.datamodel.ECADRelationInstance;
import com.cadence.adw.common.datamodel.IDatamodel;
import com.cadence.adw.common.generic.xml.datamodels.Parser;
import com.cadence.adw.conf.actions.ConfigureAdwConfRootAction;


public class HPinBlazegraphDB {
	public static void main( String[] args )
    {

		Graph graph =  (Graph) BlazeGraphFactory.open("blazegraph1.jnl");
		Parser parser = new Parser();
		boolean isParseSuccess = parser.parse("D:\\ADWSERVER172\\hp\\base.xml");
		boolean withBatchMode = true;
		System.out.println("Batch Mode Status "+(withBatchMode?"ON":"OFF"));
		long start = System.currentTimeMillis();
		if(isParseSuccess){
			HashMap metaDataObjects = parser.getMetaDataObjects();
			HashMap metaDataRelations = parser.getMetaDataRelations();
			HashMap partsDataObjects = parser.getPartsDataObjects();
			HashMap partsDataRelations = parser.getPartsDataRelations();
			try{
			
			HashMap<String,Vertex> idToVertexMap = new HashMap<String,Vertex>();
			if(withBatchMode)
			((BlazeGraph)graph).setBulkLoad(true);
			int vertexCount = 0; 
			int i=0;
			Set<String> partsDataObjectKeys= partsDataObjects.keySet();
			for(Object key: partsDataObjectKeys){
				String type = (String)key;
				List partObjects = (ArrayList) partsDataObjects.get(type);
				
				for(Object partObject: partObjects){
					IDatamodel dataModelObject = (IDatamodel)partObject;
					
					List keyValuePairs = new ArrayList<Object>();
					keyValuePairs.add(T.label);
					keyValuePairs.add(type);
					keyValuePairs.add("name");
					keyValuePairs.add(dataModelObject.getObjectName());
					for(Object entry: dataModelObject.getAttributesMap().entrySet()){
						Map.Entry property = (Map.Entry)entry;
						String attributeName = (String)property.getKey();
						String attributeValue = (String)property.getValue();
						if(attributeName.trim().isEmpty())
							continue;
						keyValuePairs.add(attributeName);
						keyValuePairs.add(attributeValue);
					}
					
					Vertex v =graph.addVertex(keyValuePairs.toArray());
					idToVertexMap.put(dataModelObject.getObjectID(), v);
					
					i++;
					vertexCount++;
					if(withBatchMode && i%10_000==0){
						((BlazeGraph)graph).setBulkLoad(false);
						((BlazeGraph)graph).tx().commit();
						((BlazeGraph)graph).setBulkLoad(true);
					}
				}
			}
			System.out.println("Vertex Count "+vertexCount);
			if(withBatchMode)
			((BlazeGraph)graph).setBulkLoad(false);
			((BlazeGraph)graph).tx().commit();
			System.out.println("Vertex insert took "+(System.currentTimeMillis()-start)+" mili sec");
			
			
			start = System.currentTimeMillis();

			if(withBatchMode)
			((BlazeGraph)graph).setBulkLoad(true);
			int edgeCount=0;
			 i=0;
			for(Object key: partsDataRelations.keySet()){
				String relationName = (String)key;
				ArrayList relationObjects = (ArrayList) partsDataRelations.get(relationName);
				for(Object relationObject: relationObjects){
					HashMap relationMap = (HashMap)relationObject;
					for(Object entry: relationMap.entrySet()){
						Map.Entry property = (Map.Entry)entry;
						ArrayList<ECADRelationInstance> relationList = (ArrayList)property.getValue();
						for(ECADRelationInstance relation: relationList){
							Vertex fromVertex = idToVertexMap.get(relation.getDatamodel().getObjectID());
							Vertex toVertex = idToVertexMap.get(relation.getRelatedDatamodel().getObjectID());
							Edge e =fromVertex.addEdge(relationName, toVertex, "fromLabel", fromVertex.label(), "toLabel", toVertex.label());
							
							i++;
							edgeCount++;
							if(withBatchMode && i%10_000==0){
								((BlazeGraph)graph).setBulkLoad(false);
								((BlazeGraph)graph).tx().commit();
								((BlazeGraph)graph).setBulkLoad(true);
							}
						}
					}
				}
			}			
			System.out.println("Edge Count "+edgeCount);
			if(withBatchMode)
			((BlazeGraph)graph).setBulkLoad(false);
			((BlazeGraph)graph).tx().commit();
			System.out.println("Edge insert took "+(System.currentTimeMillis()-start)+" mili sec");
			System.out.println("commit complete");
			graph.close();
			}catch(Exception e){
				e.printStackTrace(System.out);
			}			
		}
		else{
			System.err.println("Parsing of base.xml has been failed");
		}
		System.out.println("HP databse insert completed");
    }		
}
