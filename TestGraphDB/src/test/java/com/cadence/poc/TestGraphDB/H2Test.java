package com.cadence.poc.TestGraphDB;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.as;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.time.StopWatch;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.umlg.sqlg.structure.SqlgTransaction;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.SqlgVertex;
import org.junit.Assert;
import org.junit.Test;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.*;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonFormat.Value;

/**
 * Unit test for simple App.
 */

public class H2Test extends CommonTest {
	public H2Test() {
		super("h2.properties");
	}

	@Test
	public void testApp() {
		System.out.println(sqlgGraph.features());

		Graph gr = TinkerFactory.createModern();
		System.out.println(gr);
		Vertex one = gr.vertices(1).next();
		System.out.println(one.value("name"));
		System.out.println(one.keys());
		one.property("name", new String[] { "happy", "sad" });
		one.property("two", "1two", "two", "2two");
		System.out.println(one.value("name"));
		Vertex john = this.sqlgGraph.addVertex(T.label, "Manager", "name",
				"john", "info", new int[] { 1, 3, 4 }, "two", "1two", "two",
				"2two");
		Vertex john2 = this.sqlgGraph.addVertex(T.label, "Manager", "name",
				"john", "info", new int[] { 1, 3, 4 }, "twon", "1two", "two",
				"2two");
		Iterator<Value> v=john.values("twon");
		v.hasNext();
		john.values();
		Vertex palace1 = this.sqlgGraph.addVertex(T.label, "continent.House",
				"name", "palace1");
		Vertex corrola = this.sqlgGraph.addVertex(T.label, "fleet.Car",
				"model", "corrola");
		palace1.addEdge("managedBy", john);
		corrola.addEdge("owner", john);
		this.sqlgGraph.tx().commit();
	}
	
	@Test
	public void testSelectOnTwoVertices()
	{
		List<Object> list1 = new ArrayList<Object>();
		List<Object> list2 = new ArrayList<Object>();
		for(int i=0;i<10;i++){
			Vertex v1 = sqlgGraph.addVertex(T.label, type1, "P1_"+i,"val_"+i , "P2_"+i, i, "P3_"+i, 1.0*i);
			Vertex v2 = sqlgGraph.addVertex(T.label, type2, "Q1_"+i,"val_1"+i , "Q2_"+i, i+10, "Q3_"+i, 10.0*i);
			v1.addEdge("Edge1", v2,"dir","from v1 to v2");
			list1.add(v1.id());
			list2.add(v2.id());
		}
		sqlgGraph.tx().commit();
		for(int i=0;i<10;i++){
		GraphTraversal<Vertex, Map<String, Object>> traversal = sqlgGraph.traversal().V()
	            .hasLabel(type1)
	            .has("P1_"+i, P.eq("val_"+i)).has("P2_"+i,i).has("P3_"+i,1.0*i)
	            .as("A")
	            .out("Edge1")
	            .hasLabel(type2)
	            .has("Q1_"+i, P.eq("val_1"+i))
	            .has("Q2_"+i,i+10).has("Q3_"+i,10.0*i)
	            .as("B")
	            .select("A","B");
	            
		List<Map<String, Object>> result = traversal.toList();

		Assert.assertEquals(((Vertex)result.get(0).get("A")).id(), list1.get(i));
		Assert.assertEquals(((Vertex)result.get(0).get("B")).id(), list2.get(i));
		}
	}
	

	
	@Test
	public void testGettingVerticesofFirstType()
	{
		List<Vertex> list = new ArrayList<Vertex>();
		for(int i=0;i<300;i++){
			Vertex v1 = sqlgGraph.addVertex(T.label, type1, "P1_"+i,"val_"+i , "P2_"+i, i, "P3_"+i, 1.0*i);
			Vertex v2 = sqlgGraph.addVertex(T.label, type2, "Q1_"+i,"val_1"+i , "Q2_"+i, i+10, "Q3_"+i, 10.0*i);
			Vertex v3 = sqlgGraph.addVertex(T.label, type3, "R1_"+i,"val_10"+i , "R2_"+i, i+100, "R3_"+i, 100.0*i);	
			v1.addEdge("Edge1", v2,"dir","from v1 to v2");
			v1.addEdge("Edge2", v3,"dir","from v1 to v3");
			list.add(v1);
		}
		sqlgGraph.tx().commit();
		System.out.println(System.currentTimeMillis() - start);
		start= System.currentTimeMillis();
		for(int i=0;i<300;i++){
		GraphTraversal<Vertex, Vertex> traversal = sqlgGraph.traversal().V()
	            .hasLabel(type1)
	            .has("P1_"+i, P.eq("val_"+i)).has("P2_"+i,i).has("P3_"+i,1.0*i)
	            .as("A")
	            .out("Edge1")
	            .hasLabel(type2)
	            .has("Q1_"+i, P.eq("val_1"+i))
	            .has("Q2_"+i,i+10).has("Q3_"+i,10.0*i)
	            .as("B")
	            .and().out("Edge2")
	            .hasLabel(type3)
	            .has("R1_"+i, P.eq("val_10"+i))
	            .has("R2_"+i,100+i).has("R3_"+i,100.0*i)
	            .as("C");
		List<Vertex> result = traversal.toList();
		Assert.assertEquals(result.get(0), list.get(i));
		}
	}
	
	@Test
	public void testGetTimingForAllValuesFetched()
	{
		List<Object> list = new ArrayList<Object>();
		for(int i=0;i<100;i++){
			Vertex v1 = sqlgGraph.addVertex(T.label, type1, "P1_"+i,"val_"+i , "P2_"+i, i, "P3_"+i, 1.0*i, "P4_"+i,"value_"+i , "P5_"+i, 111.0*i);
			list.add(v1.id());
		}
		sqlgGraph.tx().commit();
		System.out.println(System.currentTimeMillis() - start);
		start= System.currentTimeMillis();
		for(int i=0;i<100;i++){
		GraphTraversal<Vertex, Vertex> traversal = sqlgGraph.traversal().V(list.get(i));     
		List<Vertex> result = traversal.toList();
		Assert.assertEquals(result.get(0).id(), list.get(i));
		}
	}
	
	@Test
	public void testGettingVerticesofAllType()
	{
		List<Object> list1 = new ArrayList<Object>();
		List<Object> list2 = new ArrayList<Object>();
		List<Object> list3 = new ArrayList<Object>();
		List<Object> list4 = new ArrayList<Object>();
		List<Object> list5 = new ArrayList<Object>();
		for(int i=0;i<400;i++){
			Vertex v1 = sqlgGraph.addVertex(T.label, type1, "P1_"+i,"val_"+i , "P2_"+i, i, "P3_"+i, 1.0*i);
			Vertex v2 = sqlgGraph.addVertex(T.label, type2, "Q1_"+i,"val_1"+i , "Q2_"+i, i+10, "Q3_"+i, 10.0*i);
			Vertex v3 = sqlgGraph.addVertex(T.label, type3, "R1_"+i,"val_10"+i , "R2_"+i, i+100, "R3_"+i, 100.0*i);
			Vertex v4 = sqlgGraph.addVertex(T.label, type2, "Q1_"+i,"val_1"+i , "Q2_"+i, i+10, "Q3_"+i, 10.0*i);
			Vertex v5 = sqlgGraph.addVertex(T.label, type3, "R1_"+i,"val_10"+i , "R2_"+i, i+100, "R3_"+i, 100.0*i);
			v1.addEdge("Edge1", v2,"dir","from v1 to v2");
			v1.addEdge("Edge2", v3,"dir","from v1 to v3");
			v1.addEdge("Edge1", v4,"dir","from v1 to v5");
			v1.addEdge("Edge2", v5,"dir","from v1 to v5");
			list1.add(v1.id());
			list2.add(v2.id());
			list3.add(v3.id());
			list4.add(v4.id());
			list5.add(v5.id());
		}
		sqlgGraph.tx().commit();
		System.out.println(System.currentTimeMillis() - start);
		start= System.currentTimeMillis();
		for(int i=0;i<400;i++){
		GraphTraversal<Vertex, Map<String, Object>> traversal = sqlgGraph.traversal().V()
				.hasLabel(type1)
	            .has("P1_"+i, P.eq("val_"+i)).has("P2_"+i,i).has("P3_"+i,1.0*i)
	            .as("A")
	            .match(
	            
	            as("A").out("Edge1")
	            .hasLabel(type2)
	            .has("Q1_"+i, P.eq("val_1"+i))
	           .has("Q2_"+i,i+10).has("Q3_"+i,10.0*i)
	           .as("B")
	            ,
	            
	            as("A").out("Edge2")
	           .hasLabel(type3)
	            .has("R1_"+i, P.eq("val_10"+i))
	            .has("R2_"+i,100+i).has("R3_"+i,100.0*i)
	            .as("C")
	            );
	            
		List<Map<String, Object>> result = traversal.toList();

		Assert.assertEquals(((Vertex)result.get(0).get("A")).id(), list1.get(i));
		Assert.assertEquals(((Vertex)result.get(0).get("B")).id(), list2.get(i));
		Assert.assertEquals(((Vertex)result.get(0).get("C")).id(), list3.get(i));
		Assert.assertEquals(((Vertex)result.get(1).get("A")).id(), list1.get(i));
		Assert.assertEquals(((Vertex)result.get(3).get("B")).id(), list4.get(i));
		Assert.assertEquals(((Vertex)result.get(3).get("C")).id(), list5.get(i));
		}
	}
	
	
	@Test
	public void testGetTimingForAllValuesFetchedForFiveColumns()
	{
		int N=10_000_000;
		List<Object> list1 = new ArrayList<Object>();
		List<Object> list2 = new ArrayList<Object>();
		for(int i=0;i<N;i++){
			Vertex v1 = sqlgGraph.addVertex(T.label, type1, "P1","val_"+i , "P2", i, "P3", 1.0*i, "P4","value"+i , "P5", 111.0*i);
			Vertex v2 = sqlgGraph.addVertex(T.label, type2, "R1","val_"+i , "R2", i, "R3", 1.0*i, "R4","value"+i , "R5", 111.0*i);
			v1.addEdge("connectted", v2);
			list1.add(v1.id());
			list2.add(v1.id());
		}
		sqlgGraph.tx().commit();
		System.out.println(System.currentTimeMillis() - start);
		start= System.currentTimeMillis();
		for(int i=0;i<N;i++){
		GraphTraversal<Vertex, Vertex> traversal = sqlgGraph.traversal().V(list1.get(i));     
		List<Vertex> result = traversal.toList();
		Assert.assertEquals(result.get(0).id(), list1.get(i));
		traversal = sqlgGraph.traversal().V(list2.get(i));     
		result = traversal.toList();
		Assert.assertEquals(result.get(0).id(), list2.get(i));
		}
	}
	@Test
	public void showNormalBatchMode() {
	    StopWatch stopWatch = new StopWatch();
	    stopWatch.start();
	    ((SqlgTransaction)sqlgGraph.tx()).normalBatchModeOn();
	    for (int i = 1; i <= 10_000_000; i++) {
	        Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "John" + i);
	        Vertex car = this.sqlgGraph.addVertex(T.label, "Car", "name", "Dodge" + i);
	        person.addEdge("drives", car);
	        if (i % 100_000 == 0) { 
	        	((SqlgTransaction)sqlgGraph.tx()).flush();
	        }
	    }
	    this.sqlgGraph.tx().commit();
	    stopWatch.stop();
	    System.out.println(stopWatch.toString());
	}
	
	@Test
	public void timingForGetByIdforTwoTypesAndFiveColumnsWithoutBatchNode()
	{
		for(int N= 10; N<=100_000; N=N*10)
		{
			System.out.println("-----Processing "+N+" vertex of two types-----");
			start = System.currentTimeMillis();
			List<Vertex> list1 = new ArrayList<Vertex>();
			List<Vertex> list2 = new ArrayList<Vertex>();
			for(int i=1;i<=N;i++){
				Vertex v1 = sqlgGraph.addVertex(T.label, type1, "P1","val_"+i , "P2", i, "P3", 1.0*i, "P4","value"+i , "P5", 111.0*i);
				Vertex v2 = sqlgGraph.addVertex(T.label, type2, "R1","val_"+i , "R2", i, "R3", 1.0*i, "R4","value"+i , "R5", 111.0*i);
				v1.addEdge("connected", v2);
				list1.add(v1);
				list2.add(v2);
			}
			this.sqlgGraph.tx().commit();
			System.out.println("Insert Time "+ (System.currentTimeMillis() - start));
			start = System.currentTimeMillis();
			
			for(int i=1;i<=N;i++){
				GraphTraversal<Vertex, Vertex> traversal = sqlgGraph.traversal().V(list1.get(i-1).id());  				
				List<Vertex> result = traversal.toList();
				Assert.assertEquals(result.get(0), list1.get(i-1));
				
				traversal = sqlgGraph.traversal().V(list2.get(i-1).id());     
				result = traversal.toList();
				Assert.assertEquals(result.get(0), list2.get(i-1));
			}

			System.out.println("Get By Id Time "+ (System.currentTimeMillis() - start));
		}
	}
}
