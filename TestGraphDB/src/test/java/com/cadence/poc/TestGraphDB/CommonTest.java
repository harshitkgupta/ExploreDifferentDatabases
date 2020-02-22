package com.cadence.poc.TestGraphDB;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.After;
import org.junit.Before;
import org.umlg.sqlg.structure.SqlgGraph;

public class CommonTest {
	public CommonTest(){}
	
	public CommonTest(final String pathToSqlgProp) {
		sqlgGraph = SqlgGraph.open(pathToSqlgProp);
	}

	public Graph sqlgGraph;
	
    final String type1 = "type1";
	final String type2 = "type2";
	final String type3 = "type3";
	
	long start;
	@Before
	public void beforeTest(){
		start = System.currentTimeMillis();
	}
	
	@After
	public void afterTest(){

	}
}
