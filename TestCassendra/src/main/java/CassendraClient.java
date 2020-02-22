import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;

public class CassendraClient {
    private static final Logger LOG = LoggerFactory.getLogger(CassendraClient.class);

    public static void main(String args[]) {
    	
        CassandraConnector connector = new CassandraConnector();
        connector.connect("127.0.0.1", null);
        Session session = connector.getSession();
        try{
        KeyspaceRepository sr = new KeyspaceRepository(session);
        sr.createKeyspace("library1", "SimpleStrategy", 1);
        sr.useKeyspace("library1");

        BookRepository br = new BookRepository(session);
        br.createTable();
        System.out.println("Table created");
        long t = System.currentTimeMillis();
        int N=500,M=1000;
        
        for(int j=0;j<M;j++)
        {
        	List<Book> books = new ArrayList<Book>();
        
	        for(int i=0;i<N;i++)
	        {
	        	Book book = new Book(UUIDs.timeBased(), "Effective Java"+i+j, "Joshua Bloch"+i+j, "Programming"+i+j);
	        	books.add(book);
	        }
	        br.insertbooks(books);
        }

        System.out.println("Time in insert"+ (System.currentTimeMillis() - t));
        t=System.currentTimeMillis();
        List<Book> l =br.selectAll();
        System.out.println("Time in select"+ (System.currentTimeMillis() - t));
        System.out.println("Total book loaded "+l.size() );

        br.deleteTable("books");
    	}
    	finally{
        connector.close();
    	}
        
        
        
    }
}