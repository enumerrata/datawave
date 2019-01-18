package datawave.microservice.accumulo.lookup;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestUtils {
    
    public static void setupTestTable(Connector connector, String tableName) throws Exception {
        if (connector.tableOperations().exists(tableName))
            connector.tableOperations().delete(tableName);
        
        assertFalse(tableName + " already exists", connector.tableOperations().exists(tableName));
        
        connector.tableOperations().create(tableName);
        assertTrue(tableName + " doesn't exist", connector.tableOperations().exists(tableName));
        
        BatchWriterConfig bwc = new BatchWriterConfig().setMaxLatency(1l, TimeUnit.SECONDS).setMaxMemory(1024l).setMaxWriteThreads(1);
        BatchWriter bw = connector.createBatchWriter(tableName, bwc);
        
        // Write 3 rows to the test table
        for (int i = 1; i < 4; i++) {
            Mutation m = new Mutation("row" + i);
            m.put("cf1", "cq1", new ColumnVisibility("A&B&C"), 1L, new Value(new byte[0]));
            m.put("cf1", "cq2", new ColumnVisibility("A&D&E"), 1L, new Value(new byte[0]));
            m.put("cf1", "cq3", new ColumnVisibility("A&F&G"), 1L, new Value(new byte[0]));
            m.put("cf2", "cq1", new ColumnVisibility("A"), 1L, new Value(new byte[0]));
            m.put("cf2", "cq2", new ColumnVisibility("B"), 1L, new Value(new byte[0]));
            m.put("cf2", "cq3", new ColumnVisibility("C"), 1L, new Value(new byte[0]));
            m.put("cf2", "cq4", new ColumnVisibility("D"), 1L, new Value(new byte[0]));
            m.put("cf2", "cq5", new ColumnVisibility("E"), 1L, new Value(new byte[0]));
            m.put("cf2", "cq6", new ColumnVisibility("F"), 1L, new Value(new byte[0]));
            m.put("cf2", "cq7", new ColumnVisibility("G"), 1L, new Value(new byte[0]));
            m.put("cf2", "cq8", new ColumnVisibility("H"), 1L, new Value(new byte[0]));
            m.put("cf2", "cq9", new ColumnVisibility("I"), 1L, new Value(new byte[0]));
            bw.addMutation(m);
        }
        bw.close();
    }
}
