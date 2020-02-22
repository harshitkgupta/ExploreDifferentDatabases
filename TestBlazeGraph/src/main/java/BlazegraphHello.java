import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.openrdf.OpenRDFException;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;

import com.bigdata.journal.Options;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.blazegraph.gremlin.structure.BlazeGraph;
import com.cadence.adw.common.datamodel.IDatamodel;
import com.cadence.adw.common.generic.xml.datamodels.Parser;



public class BlazegraphHello {
	public static void main(String[] args) throws OpenRDFException {

		final Properties props = new Properties();
		props.put(Options.BUFFER_MODE, "DiskRW"); // persistent file system located journal
		props.put(Options.FILE, "blazegraph1.jnl"); // journal file location

		final BigdataSail sail = new BigdataSail(props); // instantiate a sail
		final Repository repo = new BigdataSailRepository(sail); // create a Sesame repository

		repo.initialize();
		final BigdataGraph g = new BigdataGraphEmbedded(getOrCreateRepository("blazegraph1.jnl"));
        final String testFile = "graph-example-1.xml";

        try {
		GraphMLReader.inputGraph(graph, this.getClass().getResourceAsStream(testFile));
		for (Vertex v : graph.getVertices()) {
			System.err.println(v);
		}
		for (Edge e : graph.getEdges()) {
			System.err.println(e);
		}
     } catch (IOException e) {
		e.printStackTrace();
     }
	}
}

