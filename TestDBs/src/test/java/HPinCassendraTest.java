

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.ibm.icu.impl.duration.impl.DataRecord.EGender;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.VertexLabel;
import com.thinkaurelius.titan.core.schema.SchemaAction;
import com.thinkaurelius.titan.core.schema.SchemaStatus;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.graphdb.database.management.ManagementSystem;

public class HPinCassendraTest{
	public Graph graph;
	TitanManagement mgmt;
	long start;
	StringBuffer sb;
	
	@Rule public TestName name = new TestName();

	@Before
	public void beforeTest(){
		System.out.println("\n"+name.getMethodName());
		start = System.currentTimeMillis();
		sb = new StringBuffer();
	}
	
	@After
	public void afterTest(){
		//System.out.println(sb);
	}
	public  HPinCassendraTest() throws InterruptedException {
		graph = (Graph) TitanFactory.open("titen_cassandra_es.properties");
		mgmt = ((TitanGraph)graph).openManagement();

		graph.tx().rollback();  //Never create new indexes while a transaction is active
		VertexLabel label = mgmt.getVertexLabel("ECAD Component");
		PropertyKey	name= mgmt.getPropertyKey("tag");
		
		mgmt.buildIndex("byECADLabelOnly123", Vertex.class).addKey(name).indexOnly(label).buildCompositeIndex();
		//mgmt.updateIndex(mgmt.getGraphIndex("byNameAndLabel"), SchemaAction.REINDEX);
		mgmt.commit();	
		((ManagementSystem)mgmt).awaitGraphIndexStatus((TitanGraph)graph, "byECADLabelOnly123").status(SchemaStatus.ENABLED)
        .timeout(10, ChronoUnit.MINUTES).call();
	}
	


	@Test()
	public void selectAllPartswithThreeProperties()
	{
		GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V()
	            .hasLabel("ECAD Component").limit(10);
	            
		List<Vertex> results = traversal.toList();
		
		System.out.println("Search Time "+ (System.currentTimeMillis() - start));		
		System.out.println("Total ECAD Component fetched "+results.size());
		start=System.currentTimeMillis();
		
		for(Vertex component: results){
			sb.append("type = "+component.label());
			sb.append(" name = "+component.value("name"));
			sb.append(" revision = "+component.value("revision")+"\n");
		}
				
		System.out.println("Fetch Time "+ (System.currentTimeMillis() - start));
	}
	
	@Test
	public void selectAllPartswithAllProperties()
	{
		GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V()
	            .hasLabel("ECAD Component");
	            
		List<Vertex> results = traversal.toList();
		
		
		System.out.println("Search Time "+ (System.currentTimeMillis() - start));
		System.out.println("Total ECAD Component fetched "+results.size());
		start=System.currentTimeMillis();
		for(Vertex component: results){
			sb.append("type = "+component.label());

			Iterator<VertexProperty<Object>>  properties =component.properties();
			while(properties.hasNext())
			{
				VertexProperty vp = properties.next();
				sb.append(vp.key()+" = "+vp.value());
			}
		}				
		System.out.println("Fetch Time "+ (System.currentTimeMillis() - start));
	}
	
	@Test
	public void selectAllPartswithAllPropertiesAllRelatedObjects()
	{

		Map<Object,Vertex> ecadIdToVertexMap = new HashMap<Object,Vertex>();
		GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V().hasLabel("ECAD Component");         
		List<Vertex> ecadVertexList = traversal.toList();
		for(int i=0;i<ecadVertexList.size();i++)
		{
			ecadIdToVertexMap.put(ecadVertexList.get(i).id(), ecadVertexList.get(i));
		}
				
		GraphTraversal<Vertex, Edge> traversal12= graph.traversal().V(ecadIdToVertexMap.keySet())
				.outE();
		List<Edge> edgeList = traversal12.toList();
		Map<Object,List<Object>> ecadIdToRelatedIdMap = new HashMap<Object,List<Object>>();
		Set<Object> relatedIds = new HashSet<Object>();
		for(int i=0;i<edgeList.size(); i++)
		{
			Object fromId = edgeList.get(i).outVertex().id();
			Object toId = edgeList.get(i).inVertex().id();
			List<Object> relatedList = ecadIdToRelatedIdMap.get(fromId);
			if(relatedList == null)
			{
				relatedList = new ArrayList<Object>();
				ecadIdToRelatedIdMap.put(fromId, relatedList);
			}
			relatedList.add(toId);
			relatedIds.add(toId);
		}	
		System.out.println("Search Time "+ (System.currentTimeMillis() - start));
		start=System.currentTimeMillis();
		Map<Object,Vertex> relatedPartIdToVertexMap = new HashMap<Object,Vertex>();
		traversal = graph.traversal().V(relatedIds);	            
		List<Vertex> relatedVertexList = traversal.toList();
		for(int i=0;i<relatedVertexList.size();i++)
		{
			relatedPartIdToVertexMap.put(relatedVertexList.get(i).id(), relatedVertexList.get(i));
		}
		System.out.println("Fetch Time "+ (System.currentTimeMillis() - start));
		System.out.println("Total ECAD Component fetched "+ecadVertexList.size());
		System.out.println("Total Related Component fetched "+relatedVertexList.size());		
	}
	
	@Test
	public void selectAllPartsAndConceptHDLPartNameModelInOneQuery()
	{		
		Map<Object,Vertex> ecadIdToVertexMap = new HashMap<Object,Vertex>();
		GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V().hasLabel("ECAD Component");          
		List<Vertex> ecadVertexList = traversal.toList();
		for(int i=0;i<ecadVertexList.size();i++)
		{
			ecadIdToVertexMap.put(ecadVertexList.get(i).id(), ecadVertexList.get(i));
		}
				
		GraphTraversal<Vertex, Edge> traversal12= graph.traversal().V(ecadIdToVertexMap.keySet())
				.outE()
				.has("toLabel", "ConceptHDL Part Name Model");
		List<Edge> edgeList = traversal12.toList();
		Map<Object,List<Object>> ecadIdToConceptHDLIdMap = new HashMap<Object,List<Object>>();
		Set<Object> conceptHDlIds = new HashSet<Object>();
		for(int i=0;i<edgeList.size(); i++)
		{
			Object fromId = edgeList.get(i).outVertex().id();
			Object toId = edgeList.get(i).inVertex().id();
			List<Object> vconceptHDL = ecadIdToConceptHDLIdMap.get(fromId);
			if(vconceptHDL == null)
			{
				vconceptHDL = new ArrayList<Object>();
				ecadIdToConceptHDLIdMap.put(fromId, vconceptHDL);
			}
			vconceptHDL.add(toId);
			conceptHDlIds.add(toId);
		}	
		System.out.println("Search Time "+ (System.currentTimeMillis() - start));
		start=System.currentTimeMillis();
				
		Map<Object,Vertex> conceptHDLIdToVertexMap = new HashMap<Object,Vertex>();
		traversal = graph.traversal().V(conceptHDlIds);	            
		List<Vertex> conceptHDlVertexList = traversal.toList();
		for(int i=0;i<conceptHDlVertexList.size();i++)
		{
			conceptHDLIdToVertexMap.put(conceptHDlVertexList.get(i).id(), conceptHDlVertexList.get(i));
		}
		System.out.println("Fetch Time "+ (System.currentTimeMillis() - start));
		System.out.println("Total ECAD Component fetched "+ecadVertexList.size());
		System.out.println("Total Concept HDL Component fetched "+conceptHDlVertexList.size());
	}
	
	@Test
	public void selectAllPartsAndConceptHDLPartNameModelIndividual()
	{
		GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V()
	            .hasLabel("ECAD Component");	            
		List<Vertex> ecadVertexList = traversal.toList();
		Map<Object,Vertex> ecadIdToVertexMap = new HashMap<Object,Vertex>();
		Map<Object,List<Object>> ecadIdToConceptHDLIdMap = new HashMap<Object,List<Object>>();
		Set<Object> conceptHDlIds = new HashSet<Object>();
		for(int i=0;i<ecadVertexList.size();i++)
		{
			Object ecadId = ecadVertexList.get(i).id();
			ecadIdToVertexMap.put(ecadId, ecadVertexList.get(i));
			GraphTraversal<Vertex, Object> traversal1 = graph.traversal().V(ecadId)
							.out().hasLabel("ConceptHDL Part Name Model").id();
			List<Object> vconceptHDL = traversal1.toList();
			ecadIdToConceptHDLIdMap.put(ecadId,vconceptHDL);
			conceptHDlIds.addAll(vconceptHDL);
		}	
		traversal = graph.traversal().V(conceptHDlIds);	            
		List<Vertex> conceptHDlVertexList = traversal.toList();
		Map<Object,Vertex> conceptHDLIdToVertexMap = new HashMap<Object,Vertex>();
		for(int i=0;i<conceptHDlVertexList.size();i++)
		{
			conceptHDLIdToVertexMap.put(conceptHDlVertexList.get(i).id(), conceptHDlVertexList.get(i));
		}	
		System.out.println("Search Time "+ (System.currentTimeMillis() - start));
		System.out.println("Total ECAD Component fetched "+ecadVertexList.size());
		System.out.println("Total Concept HDL Component fetched "+conceptHDlVertexList.size());
	}
	
	
	@Test
	public void selectAllPartsAndConceptHDLPartNameAndAllegroFootprintModelUsingIdOfEachType()
	{	
		Map<Object,Vertex> ecadIdToVertexMap = new HashMap<Object,Vertex>();
		GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V()
				.hasLabel("ECAD Component");          
		List<Vertex> ecadVertexList = traversal.toList();
		for(int i=0;i<ecadVertexList.size();i++)
		{
			ecadIdToVertexMap.put(ecadVertexList.get(i).id(), ecadVertexList.get(i));
		}
		
		Map<Object,Vertex> conceptHDLIdToVertexMap = new HashMap<Object,Vertex>();
		traversal = graph.traversal().V()
				.hasLabel("ConceptHDL Part Name Model");	            
		List<Vertex> conceptHDlVertexList = traversal.toList();
		for(int i=0;i<conceptHDlVertexList.size();i++)
		{
			conceptHDLIdToVertexMap.put(conceptHDlVertexList.get(i).id(), conceptHDlVertexList.get(i));
		}
		
		
		Map<Object,Vertex> allegroIdToVertexMap = new HashMap<Object,Vertex>();
		traversal = graph.traversal().V()
				.hasLabel("Allegro Footprint Model");	            
		List<Vertex> allegroVertexList = traversal.toList();
		for(int i=0;i<allegroVertexList.size();i++)
		{
			allegroIdToVertexMap.put(allegroVertexList.get(i).id(), allegroVertexList.get(i));
		}
		
		Set<Object> conceptHDlIds = conceptHDLIdToVertexMap.keySet();
		Set<Object> allegroIds = allegroIdToVertexMap.keySet();
		Set<Object> ecadIds = ecadIdToVertexMap.keySet(); 
						
		GraphTraversal<Vertex, Edge> traversal12= graph.traversal().V(ecadIds)
				.outE()
				.has("toLabel", "ConceptHDL Part Name Model");
		List<Edge> edge12List = traversal12.toList();
		Map<Object,List<Object>> ecadIdToConceptHDLIdMap = new HashMap<Object,List<Object>>();
		
		for(int i=0;i<edge12List.size(); i++)
		{
			Object fromId = edge12List.get(i).outVertex().id();
			Object toId = edge12List.get(i).inVertex().id();
			if(!conceptHDlIds.contains(toId))
				continue;
			List<Object> vconceptHDL = ecadIdToConceptHDLIdMap.get(fromId);
			if(vconceptHDL == null)
			{
				vconceptHDL = new ArrayList<Object>();
				ecadIdToConceptHDLIdMap.put(fromId, vconceptHDL);
			}
			vconceptHDL.add(toId);
		}	
		
		
		
		GraphTraversal<Vertex, Edge> traversal13= graph.traversal().V(ecadIds)
				.outE()
				.has("toLabel", "Allegro Footprint Model");
		List<Edge> edge13List = traversal13.toList();
		Map<Object,List<Object>> ecadIdToAllegroIdMap = new HashMap<Object,List<Object>>();
		
		for(int i=0;i<edge13List.size(); i++)
		{
			Object fromId = edge13List.get(i).outVertex().id();
			Object toId = edge13List.get(i).inVertex().id();
			if(!allegroIds.contains(toId))
				continue;
			List<Object> vconceptHDL = ecadIdToAllegroIdMap.get(fromId);
			if(vconceptHDL == null)
			{
				vconceptHDL = new ArrayList<Object>();
				ecadIdToAllegroIdMap.put(fromId, vconceptHDL);
			}
			vconceptHDL.add(toId);
		}
					
		System.out.println("Fetch Time "+ (System.currentTimeMillis() - start));
		System.out.println("Total ECAD Component fetched "+ecadVertexList.size());
		System.out.println("Total Concept HDL Component fetched "+conceptHDlVertexList.size());
		System.out.println("Total Allegro Footprint Component fetched "+allegroVertexList.size());
		System.out.println("ECAD to Concept HDL fetched "+ecadIdToConceptHDLIdMap.size());
		System.out.println("ECAD to Allegro Footprint fetched "+ecadIdToAllegroIdMap.size());
	}
	@Test
	public void selectAllPartsAndConceptHDLPartNameAndAllegroFootprintModelUsingEachType()
	{	
//		GraphTraversal<Vertex, Object> ecadTraversal= sqlgGraph.traversal().V()
//				.has("ECAD Component").id().limit(2000);
//		List<Object> ecadIds = ecadTraversal.toList();
//		
//		GraphTraversal<Vertex, Object> conceptHdlTraversal= sqlgGraph.traversal().V()
//				.has("ConceptHDL Part Name Model").id().limit(100);
//	//	List<Object> conceptHDlIds = conceptHdlTraversal.toList();
//		
//		GraphTraversal<Vertex, Object> allegroTraversal= sqlgGraph.traversal().V()
//				.has("Allegro Footprint Model").id().limit(100);
//		List<Object> allegroIds = allegroTraversal.toList();
		
		Map<Object,Vertex> ecadIdToVertexMap = new HashMap<Object,Vertex>();
		GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V()
				.hasLabel("ECAD Component");          
		List<Vertex> ecadVertexList = traversal.toList();
		for(int i=0;i<ecadVertexList.size();i++)
		{
			ecadIdToVertexMap.put(ecadVertexList.get(i).id(), ecadVertexList.get(i));
		}
				
		GraphTraversal<Vertex, Edge> traversal12= graph.traversal().V(ecadIdToVertexMap.keySet())
				.outE()
				.has("toLabel", "ConceptHDL Part Name Model");
		List<Edge> edge12List = traversal12.toList();
		Map<Object,List<Object>> ecadIdToConceptHDLIdMap = new HashMap<Object,List<Object>>();
		Set<Object> conceptHDlIds = new HashSet<Object>();
		for(int i=0;i<edge12List.size(); i++)
		{
			Object fromId = edge12List.get(i).outVertex().id();
			Object toId = edge12List.get(i).inVertex().id();
			List<Object> vconceptHDL = ecadIdToConceptHDLIdMap.get(fromId);
			if(vconceptHDL == null)
			{
				vconceptHDL = new ArrayList<Object>();
				ecadIdToConceptHDLIdMap.put(fromId, vconceptHDL);
			}
			vconceptHDL.add(toId);
			conceptHDlIds.add(toId);
		}	
		
		
		
		GraphTraversal<Vertex, Edge> traversal13= graph.traversal().V(ecadIdToVertexMap.keySet())
				.outE()
				.has("toLabel", "AllegroFootprint Model");
		List<Edge> edge13List = traversal13.toList();
		Map<Object,List<Object>> ecadIdToAllegroIdMap = new HashMap<Object,List<Object>>();
		Set<Object> allegroIds = new HashSet<Object>();
		for(int i=0;i<edge13List.size(); i++)
		{
			Object fromId = edge13List.get(i).outVertex().id();
			Object toId = edge13List.get(i).inVertex().id();
			List<Object> vconceptHDL = ecadIdToAllegroIdMap.get(fromId);
			if(vconceptHDL == null)
			{
				vconceptHDL = new ArrayList<Object>();
				ecadIdToAllegroIdMap.put(fromId, vconceptHDL);
			}
			vconceptHDL.add(toId);
			allegroIds.add(toId);
		}
		System.out.println("Search Time "+ (System.currentTimeMillis() - start));
		start=System.currentTimeMillis();
				
		Map<Object,Vertex> conceptHDLIdToVertexMap = new HashMap<Object,Vertex>();
		traversal = graph.traversal().V(conceptHDlIds);	            
		List<Vertex> conceptHDlVertexList = traversal.toList();
		for(int i=0;i<conceptHDlVertexList.size();i++)
		{
			conceptHDLIdToVertexMap.put(conceptHDlVertexList.get(i).id(), conceptHDlVertexList.get(i));
		}
		
		
		Map<Object,Vertex> allegroIdToVertexMap = new HashMap<Object,Vertex>();
		traversal = graph.traversal().V(allegroIds);	            
		List<Vertex> allegroVertexList = traversal.toList();
		for(int i=0;i<allegroVertexList.size();i++)
		{
			allegroIdToVertexMap.put(allegroVertexList.get(i).id(), allegroVertexList.get(i));
		}
		
		System.out.println("Fetch Time "+ (System.currentTimeMillis() - start));
		System.out.println("Total ECAD Component fetched "+ecadVertexList.size());
		System.out.println("Total Concept HDL Component fetched "+conceptHDlVertexList.size());
		System.out.println("Total Allegro HDL Component fetched "+allegroVertexList.size());
	}
	

	
	@Test
	public void selectAllPartsAndConceptHDLPartNameModelWithPartNumber()
	{	
		
		Map<Object,Vertex> ecadIdToVertexMap = new HashMap<Object,Vertex>();
		GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V().hasLabel("ECAD Component")
				//.has("name",Text.ncontains("shan"))
				;          
		List<Vertex> ecadVertexList = traversal.toList();
		for(int i=0;i<ecadVertexList.size();i++)
		{
			ecadIdToVertexMap.put(ecadVertexList.get(i).id(), ecadVertexList.get(i));
		}
				
		GraphTraversal<Vertex, Edge> traversal12= graph.traversal().V(ecadIdToVertexMap.keySet())
				.outE()
				.has("fromLabel", "ECAD Component")
				.or(has("toLabel", "ConceptHDL Part Name Model"));
		List<Edge> edgeList = traversal12.toList();
		Map<Object,List<Object>> ecadIdToConceptHDLIdMap = new HashMap<Object,List<Object>>();
		Set<Object> conceptHDlIds = new HashSet<Object>();
		for(int i=0;i<edgeList.size(); i++)
		{
			Object fromId = edgeList.get(i).outVertex().id();
			Object toId = edgeList.get(i).inVertex().id();
			List<Object> vconceptHDL = ecadIdToConceptHDLIdMap.get(fromId);
			if(vconceptHDL == null)
			{
				vconceptHDL = new ArrayList<Object>();
				ecadIdToConceptHDLIdMap.put(fromId, vconceptHDL);
			}
			vconceptHDL.add(toId);
			conceptHDlIds.add(toId);
		}	
		
		
		Map<Object,Vertex> conceptHDLIdToVertexMap = new HashMap<Object,Vertex>();
		traversal = graph.traversal().V(conceptHDlIds);	            
		List<Vertex> conceptHDlVertexList = traversal.toList();
		for(int i=0;i<conceptHDlVertexList.size();i++)
		{
			conceptHDLIdToVertexMap.put(conceptHDlVertexList.get(i).id(), conceptHDlVertexList.get(i));
		}
		System.out.println("Search Time "+ (System.currentTimeMillis() - start));
		System.out.println("Total ECAD Component fetched "+ecadVertexList.size());
		System.out.println("Total Concept HDL Component fetched "+conceptHDlVertexList.size());
	}
}
