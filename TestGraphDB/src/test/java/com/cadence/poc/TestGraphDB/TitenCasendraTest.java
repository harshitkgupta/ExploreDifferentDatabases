package com.cadence.poc.TestGraphDB;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.VertexLabel;
import com.thinkaurelius.titan.core.schema.EdgeLabelMaker;
import com.thinkaurelius.titan.core.schema.PropertyKeyMaker;
import com.thinkaurelius.titan.graphdb.internal.Order;
import com.thinkaurelius.titan.core.schema.TitanManagement;

public class TitenCasendraTest{

	Graph g;
	Long start;
	public TitenCasendraTest(){
		g = (Graph) TitanFactory.open("titen_cassandra_es.properties");
//		TitanGraph tg = ((TitanGraph)g);
//		//PropertyKey entityId = ((PropertyKeyMaker) tg.makeVertexLabel("type1")).dataType(String.class).make();
//		EdgeLabel edgeId = (EdgeLabel) tg.makeEdgeLabel("connected").make();
//		TitanManagement tm = tg.openManagement();
//		//tm.buildEdgeIndex(edgeId, "connectedRel", Direction.BOTH);
//		((Transaction) tg).commit();
	}
	
	@Test
	public void timingForGetByIdforTwoTypesAndFiveColumnsWithoutBatchNode()
	{
		for(int N= 10; N<=10; N=N*10)
		{
			System.out.println("-----Processing "+N+" vertex of two types-----");
			start = System.currentTimeMillis();
			List<Vertex> list1 = new ArrayList<Vertex>();
			List<Vertex> list2 = new ArrayList<Vertex>();
			for(int i=1;i<=N;i++){
				Vertex v1 = g.addVertex(T.label, "type1", "P1","val_"+i , "P2", i, "P3", 1.0*i, "P4","value"+i , "P5", 111.0*i);
				Vertex v2 = g.addVertex(T.label, "type2", "R1","val_"+i , "R2", i, "R3", 1.0*i, "R4","value"+i , "R5", 111.0*i);
				v1.addEdge("connected", v2);
				list1.add(v1);
				list2.add(v2);
				if (i%1000== 0) { 
		        	g.tx().commit();
		        }
			}
			System.out.println("Insert Time "+ (System.currentTimeMillis() - start));
			start = System.currentTimeMillis();
			
			for(int i=1;i<=N;i++){
				GraphTraversal<Vertex, Vertex> traversal = g.traversal().V(list1.get(i-1).id()).has("P1","val_"+i);  				
				List<Vertex> result = traversal.toList();
				Assert.assertEquals(result.get(0), list1.get(i-1));
				
				traversal = g.traversal().V(list2.get(i-1).id());     
				result = traversal.toList();
				Assert.assertEquals(result.get(0), list2.get(i-1));
			}

			System.out.println("Get By Id Time "+ (System.currentTimeMillis() - start));
		}
		
	}
	
	@Test
	public void timingForSearchByPropertyforTwoTypesAndFiveColumns()
	{
		int N= 100_000;
			System.out.println("-----Processing "+N+" vertex of two types-----");
			start = System.currentTimeMillis();
			List<Vertex> list1 = new ArrayList<Vertex>();
			List<Vertex> list2 = new ArrayList<Vertex>();
			g.tx().open();
			for(int i=1;i<=N;i++){
				Vertex v1 = g.addVertex(T.label, "type1", "P1","val_"+i , "P2", i, "P3", 1.0*i, "P4","value"+i , "P5", 111.0*i);
				Vertex v2 = g.addVertex(T.label, "type2", "R1","val_"+i , "R2", i, "R3", 1.0*i, "R4","value"+i , "R5", 111.0*i);
				v1.addEdge("connected", v2);
				list1.add(v1);
				list2.add(v2);
				if (i%10000== 0) { 
		        	g.tx().commit();
		        }
			}
			g.tx().commit();
			System.out.println("Insert Time "+ (System.currentTimeMillis() - start));
			start = System.currentTimeMillis();
			
			for(int i=1;i<=N;i++){
				GraphTraversal<Vertex, Map<String, Object>> traversal = g.traversal().V()
			            .hasLabel("type1")
			            .has("P1","val_"+i).has("P2", i).has("P3", 1.0*i)
			            .as("A")
			            .out("connected")
			            .hasLabel("type2")
			            .has("R1","val_"+i).has("R2", i).has("R3", 1.0*i)
			            .as("B")
			            .select("A","B");
			            
				List<Map<String, Object>> result = traversal.toList();

				//Assert.assertEquals(((Vertex)result.get(result.size()-1).get("A")), list1.get(i));
				//Assert.assertEquals(((Vertex)result.get(result.size()-1).get("B")), list2.get(i));
			}

			System.out.println("Get By Id Time "+ (System.currentTimeMillis() - start));
	}

}
