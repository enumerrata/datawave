package datawave.query.iterator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class SourceManagerTest {
    private static final SimpleDateFormat shardFormatter = new SimpleDateFormat("yyyyMMdd HHmmss");
    private static long ts = -1;
    
    private SortedMapIterator data;
    private SourceCounter counter;
    
    @Before
    public void setup() throws ParseException, IOException {
        
        data = new SortedMapIterator(createTestData());
        
        counter = new SourceCounter();
        
        Map<String,String> options = Maps.newHashMap();
        
        counter.init(data, options, new MockIteratorEnvironment());
    }
    
    @Test
    public void ensureOnlyOne() {
        SourceManager manager = new SourceManager(counter);
        manager.setInitialSize(1);
        
        for (int i = 0; i < 250; i++) {
            manager.deepCopy(null);
        }
        
        assertEquals(1, counter.counter);
    }
    
    @Test
    public void assertMax() {
        SourceManager manager = new SourceManager(counter);
        manager.setInitialSize(21);
        
        for (int i = 0; i < 250; i++) {
            manager.deepCopy(null);
        }
        
        assertEquals(21, counter.counter);
    }
    
    @Test
    public void assertNexts() throws IOException {
        SourceManager manager = new SourceManager(counter);
        manager.setInitialSize(10);
        
        Collection<SortedKeyValueIterator<Key,Value>> kvList = Lists.newArrayList();
        for (int i = 0; i < 500; i++) {
            kvList.add(manager.deepCopy(null));
        }
        
        for (SortedKeyValueIterator<Key,Value> kv : kvList) {
            kv.seek(new Range(), Collections.emptyList(), false);
            kv.next();
        }
        
        assertEquals(500, counter.nextCalls);
        assertEquals(10, counter.counter);
    }
    
    @Test
    public void ensureDeepCopiesCalledLazily() throws IOException {
        SourceMaker maker = new SourceMaker();
        SourceManager manager = new SourceManager(maker);
        manager.setInitialSize(10);
        
        Collection<SortedKeyValueIterator<Key,Value>> kvList = Lists.newArrayList();
        for (int i = 0; i < 8; i++) {
            kvList.add(manager.deepCopy(null));
        }
        
        // we create one when we start
        
        assertEquals(9, manager.getChildrenSize());
        
        assertEquals(9, manager.getCreatedSize());
        
        assertEquals(9, maker.children.size());
    }
    
    public static SortedMap<Key,Value> createTestData() throws ParseException {
        return createTestData("");
    }
    
    public static SortedMap<Key,Value> createTestData(String preId) throws ParseException {
        
        shardFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
        ts = shardFormatter.parse("20121126 123023").getTime();
        long ts2 = ts + 10000;
        long ts3 = ts + 200123;
        
        TreeMap<Key,Value> map = Maps.newTreeMap();
        
        map.put(new Key("20121126_0", "fi\0" + "FOO", "bar\0" + "foobar\0" + preId + 1, ts), new Value(new byte[0]));
        map.put(new Key("20121126_0", "fi\0" + "FOO", "bar\0" + "foobar\0" + preId + 2, ts), new Value(new byte[0]));
        map.put(new Key("20121126_0", "fi\0" + "FOO", "bar\0" + "foobar\0" + preId + 3, ts), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + preId + 1, "FOO\0bar", ts), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + preId + 1, "BAR\0foo", ts2), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + preId + 2, "FOO\0bar", ts), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + preId + 2, "BAR\0foo", ts2), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + preId + 3, "FOO\0bar", ts), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + preId + 3, "BAR\0foo", ts2), new Value(new byte[0]));
        
        map.put(new Key("20121126_1", "fi\0" + "FOO", "bar\0" + "foobar\0" + preId + 4, ts), new Value(new byte[0]));
        map.put(new Key("20121126_1", "fi\0" + "FOO", "bar\0" + "foobar\0" + preId + 5, ts), new Value(new byte[0]));
        map.put(new Key("20121126_1", "fi\0" + "FOO", "bar\0" + "foobar\0" + preId + 6, ts), new Value(new byte[0]));
        map.put(new Key("20121126_1", "foobar\0" + preId + 4, "FOO\0bar", ts), new Value(new byte[0]));
        map.put(new Key("20121126_1", "foobar\0" + preId + 5, "FOO\0bar", ts), new Value(new byte[0]));
        map.put(new Key("20121126_1", "foobar\0" + preId + 5, "BAR\0foo", ts2), new Value(new byte[0]));
        map.put(new Key("20121126_1", "foobar\0" + preId + 6, "FOO\0bar", ts), new Value(new byte[0]));
        
        map.put(new Key("20121126_2", "fi\0" + "FOO", "bar\0" + "foobar\0" + preId + 7, ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "fi\0" + "FOO", "bar\0" + "foobar\0" + preId + 8, ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "fi\0" + "FOO", "bar\0" + "foobar\0" + preId + 9, ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + preId + 7, "FOO\0bar", ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + preId + 7, "BAR\0foo", ts3), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + preId + 8, "FOO\0bar", ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + preId + 8, "BAR\0foo", ts3), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + preId + 9, "FOO\0bar", ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + preId + 9, "BAR\0foo", ts3), new Value(new byte[0]));
        map.put(new Key("20121126_3", "fi\0" + "FOOSICKLES", "bar\0" + "foobar\0" + 33, ts), new Value(new byte[0]));
        
        return map;
    }
    
    public class SourceCounter extends org.apache.accumulo.core.iterators.WrappingIterator {
        long counter = 0;
        
        long nextCalls = 0;
        
        @Override
        public void next() {
            nextCalls++;
        }
        
        @Override
        public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
            counter++;
            return this;
        }
    }
    
    public class SourceMaker extends org.apache.accumulo.core.iterators.WrappingIterator {
        
        protected List<SourceMaker> children = Lists.newArrayList();
        long nextCalls = 0;
        
        @Override
        public void next() {
            nextCalls++;
        }
        
        @Override
        public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
            SourceMaker maker = new SourceMaker();
            children.add(maker);
            return maker;
        }
    }
    
    public class MockIteratorEnvironment implements IteratorEnvironment {
        
        AccumuloConfiguration conf;
        
        public MockIteratorEnvironment(AccumuloConfiguration conf) {
            this.conf = conf;
        }
        
        public MockIteratorEnvironment() {
            this.conf = AccumuloConfiguration.getDefaultConfiguration();
        }
        
        @Override
        public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String mapFileName) throws IOException {
            Configuration conf = CachedConfiguration.getInstance();
            FileSystem fs = FileSystem.get(conf);
            return RFileOperations.getInstance().newReaderBuilder().forFile(mapFileName, fs, conf)
                            .withTableConfiguration(AccumuloConfiguration.getDefaultConfiguration()).seekToBeginning().build();
        }
        
        @Override
        public AccumuloConfiguration getConfig() {
            return conf;
        }
        
        @Override
        public IteratorScope getIteratorScope() {
            return IteratorScope.scan;
        }
        
        @Override
        public boolean isFullMajorCompaction() {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public Authorizations getAuthorizations() {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public IteratorEnvironment cloneWithSamplingEnabled() {
            throw new SampleNotPresentException();
        }
        
        @Override
        public boolean isSamplingEnabled() {
            return false;
        }
        
        @Override
        public SamplerConfiguration getSamplerConfiguration() {
            return null;
        }
        
        @Override
        public void registerSideChannel(SortedKeyValueIterator<Key,Value> iter) {
            throw new UnsupportedOperationException();
        }
        
    }
}
