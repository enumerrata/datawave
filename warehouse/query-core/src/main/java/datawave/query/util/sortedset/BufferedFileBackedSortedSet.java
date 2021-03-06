package datawave.query.util.sortedset;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import datawave.query.util.sortedset.FileSortedSet.SortedSetFileHandler;
import org.apache.log4j.Logger;

/**
 * This is a sorted set that will hold up to a specified number of entries before flushing the data to disk. Files will be created as needed. An additional
 * "persist" call is supplied to force flushing to disk. The iterator.remove and the subset operations will work up until any buffer has been flushed to disk.
 * After that, those operations will not work as specified by the underlying FileSortedSet.
 * 
 * @param <E>
 */
public class BufferedFileBackedSortedSet<E extends Serializable> implements SortedSet<E> {
    private static final Logger log = Logger.getLogger(BufferedFileBackedSortedSet.class);
    protected static final int DEFAULT_BUFFER_PERSIST_THRESHOLD = 1000;
    protected static final int DEFAULT_MAX_OPEN_FILES = 100;
    
    protected MultiSetBackedSortedSet<E> set = new MultiSetBackedSortedSet<>();
    protected int maxOpenFiles = 10000;
    protected FileSortedSet<E> buffer = null;
    protected Comparator<? super E> comparator = null;
    protected boolean sizeModified = false;
    protected int size = 0;
    
    protected SortedSetFileHandlerFactory handlerFactory;
    protected int bufferPersistThreshold;
    
    /**
     * A factory for SortedSetFileHandlers
     * 
     * 
     * 
     */
    public interface SortedSetFileHandlerFactory {
        SortedSetFileHandler createHandler() throws IOException;
    }
    
    public BufferedFileBackedSortedSet(BufferedFileBackedSortedSet<E> other) {
        this(other.comparator, other.bufferPersistThreshold, other.maxOpenFiles, other.handlerFactory);
        for (SortedSet<E> subSet : other.set.getSets()) {
            FileSortedSet<E> clone = new FileSortedSet<>((FileSortedSet<E>) subSet);
            this.set.addSet(clone);
            if (!clone.isPersisted()) {
                this.buffer = clone;
            }
        }
        this.sizeModified = other.sizeModified;
        this.size = other.size;
    }
    
    public BufferedFileBackedSortedSet(SortedSetFileHandlerFactory handlerFactory) {
        this(null, DEFAULT_BUFFER_PERSIST_THRESHOLD, DEFAULT_MAX_OPEN_FILES, handlerFactory);
    }
    
    public BufferedFileBackedSortedSet(Comparator<? super E> comparator, SortedSetFileHandlerFactory handlerFactory) {
        this(comparator, DEFAULT_BUFFER_PERSIST_THRESHOLD, DEFAULT_MAX_OPEN_FILES, handlerFactory);
    }
    
    public BufferedFileBackedSortedSet(Comparator<? super E> comparator, int bufferPersistThreshold, int maxOpenFiles,
                    SortedSetFileHandlerFactory handlerFactory) {
        this.comparator = comparator;
        this.handlerFactory = handlerFactory;
        this.bufferPersistThreshold = bufferPersistThreshold;
        this.maxOpenFiles = maxOpenFiles;
    }
    
    public void persist() throws IOException {
        if (buffer != null) {
            buffer.persist();
            buffer = null;
        }
    }
    
    protected List<FileSortedSet<E>> getSets() {
        List<FileSortedSet<E>> sets = new ArrayList<>();
        for (SortedSet<E> subSet : set.getSets()) {
            sets.add((FileSortedSet<E>) subSet);
        }
        return sets;
    }
    
    protected void addSet(FileSortedSet<E> subSet) {
        set.addSet(subSet);
    }
    
    public boolean hasPersistedData() {
        for (SortedSet<E> subSet : set.getSets()) {
            if (((FileSortedSet<E>) subSet).isPersisted()) {
                return true;
            }
        }
        return false;
    }
    
    public boolean isPersisted() {
        // we are (completely) persisted iff the buffer is persisted
        return (buffer == null || buffer.isPersisted());
    }
    
    @Override
    public int size() {
        if (sizeModified) {
            this.size = set.size();
            sizeModified = false;
        }
        return this.size;
    }
    
    public int getBufferPersistThreshold() {
        return this.bufferPersistThreshold;
    }
    
    public int getBufferSize() {
        return (this.buffer == null ? 0 : this.buffer.size());
    }
    
    @Override
    public boolean isEmpty() {
        return size() == 0;
    }
    
    @Override
    public boolean contains(Object o) {
        // try the cheap operation first
        if (buffer != null && buffer.contains(o)) {
            return true;
        } else {
            return set.contains(o);
        }
    }
    
    @Override
    public boolean containsAll(Collection<?> c) {
        // try the cheap operation first
        if (buffer != null && buffer.containsAll(c)) {
            return true;
        } else {
            return set.containsAll(c);
        }
    }
    
    @Override
    public Iterator<E> iterator() {
        // first lets compact down the sets if needed
        try {
            // if we have any persisted sets, then ensure we are persisted
            if (set.getSets().size() > 1) {
                persist();
            }
            compact(maxOpenFiles);
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to compact file backed sorted set", ioe);
        }
        return set.iterator();
    }
    
    public void compact(int maxFiles) throws IOException {
        // if we have more sets than we are allowed, then we need to compact this down
        if (maxFiles > 0 && set.getSets().size() > maxFiles) {
            if (log.isDebugEnabled()) {
                log.debug("Compacting " + handlerFactory);
            }
            while (set.getSets().size() > maxFiles) {
                List<SortedSet<E>> sets = set.getSets();
                int numSets = sets.size();
                // we want to do layers of compactions up to ivaratorMaxOpenFiles at a time until the total
                // number of files is under maxFiles
                int setsPerCompaction = Math.max(2, Math.min(maxOpenFiles, Math.round((float) numSets / maxFiles)));
                int iterationsWithExtra = numSets / setsPerCompaction;
                
                MultiSetBackedSortedSet<E> newSet = new MultiSetBackedSortedSet<>();
                MultiSetBackedSortedSet<E> setToCompact = new MultiSetBackedSortedSet<>();
                
                int iteration = 0;
                byte subtraction = (byte) (iteration < iterationsWithExtra ? 0 : 1);
                for (int i = 0; i < numSets; i++) {
                    setToCompact.addSet(sets.get(i));
                    if (setToCompact.getSets().size() == (setsPerCompaction - subtraction)) {
                        newSet.addSet(compact(setToCompact));
                        setToCompact.clear();
                        iteration++;
                        subtraction = (byte) (iteration < iterationsWithExtra ? 0 : 1);
                    }
                }
                if (!setToCompact.isEmpty()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Starting compaction for " + setToCompact);
                    }
                    long start = System.currentTimeMillis();
                    FileSortedSet<E> compaction = compact(setToCompact);
                    if (log.isDebugEnabled()) {
                        long delta = System.currentTimeMillis() - start;
                        log.debug("Compacted " + setToCompact + " -> " + compaction + " in " + delta + "ms");
                    }
                    newSet.addSet(compaction);
                    setToCompact.clear();
                }
                this.set = newSet;
            }
        }
    }
    
    private FileSortedSet<E> compact(MultiSetBackedSortedSet<E> setToCompact) throws IOException {
        return new FileSortedSet<>(setToCompact, handlerFactory.createHandler(), true);
    }
    
    @Override
    public Object[] toArray() {
        return set.toArray();
    }
    
    @Override
    public <T> T[] toArray(T[] a) {
        return set.toArray(a);
    }
    
    @Override
    public boolean add(E e) {
        if (buffer == null) {
            try {
                buffer = new FileSortedSet<>(comparator, handlerFactory.createHandler(), false);
            } catch (Exception ex) {
                throw new IllegalStateException("Unable to create an underlying FileSortedSet", ex);
            }
            
            set.addSet(buffer);
        }
        if (buffer.add(e)) {
            sizeModified = true;
            if (buffer.size() >= bufferPersistThreshold) {
                try {
                    buffer.persist();
                } catch (Exception ex) {
                    throw new IllegalStateException("Unable to persist FileSortedSet", ex);
                }
                buffer = null;
            }
            return true;
        }
        return false;
    }
    
    @Override
    public boolean addAll(Collection<? extends E> c) {
        if (buffer == null) {
            try {
                buffer = new FileSortedSet<>(comparator, handlerFactory.createHandler(), false);
            } catch (Exception ex) {
                throw new IllegalStateException("Unable to create an underlying FileSortedSet", ex);
            }
            set.addSet(buffer);
        }
        if (buffer.addAll(c)) {
            sizeModified = true;
            if (buffer.size() >= bufferPersistThreshold) {
                try {
                    buffer.persist();
                } catch (Exception ex) {
                    throw new IllegalStateException("Unable to persist FileSortedSet", ex);
                }
                buffer = null;
            }
            return true;
        }
        return false;
    }
    
    @Override
    public boolean remove(Object o) {
        boolean removed = false;
        for (SortedSet<E> subSet : set.getSets()) {
            FileSortedSet<E> fileSet = (FileSortedSet<E>) subSet;
            if (fileSet.contains(o)) {
                if (fileSet.isPersisted()) {
                    try {
                        fileSet.load();
                        fileSet.remove(o);
                        fileSet.persist();
                        removed = true;
                    } catch (Exception e) {
                        throw new IllegalStateException("Unable to remove item from underlying files", e);
                    }
                } else {
                    if (fileSet.remove(o)) {
                        removed = true;
                    }
                }
            }
        }
        if (removed) {
            this.sizeModified = true;
        }
        return removed;
    }
    
    @Override
    public boolean retainAll(Collection<?> c) {
        boolean modified = false;
        for (SortedSet<E> subSet : set.getSets()) {
            FileSortedSet<E> fileSet = (FileSortedSet<E>) subSet;
            if (fileSet.isPersisted()) {
                try {
                    fileSet.load();
                    if (fileSet.retainAll(c)) {
                        modified = true;
                    }
                    fileSet.persist();
                } catch (Exception e) {
                    throw new IllegalStateException("Unable to remove item from underlying files", e);
                }
            } else {
                if (fileSet.retainAll(c)) {
                    modified = true;
                }
            }
        }
        if (modified) {
            this.sizeModified = true;
        }
        return modified;
    }
    
    @Override
    public boolean removeAll(Collection<?> c) {
        boolean modified = false;
        for (SortedSet<E> subSet : set.getSets()) {
            FileSortedSet<E> fileSet = (FileSortedSet<E>) subSet;
            if (fileSet.isPersisted()) {
                try {
                    fileSet.load();
                    if (fileSet.removeAll(c)) {
                        modified = true;
                    }
                    fileSet.persist();
                } catch (Exception e) {
                    throw new IllegalStateException("Unable to remove item from underlying files", e);
                }
            } else {
                if (fileSet.removeAll(c)) {
                    modified = true;
                }
            }
        }
        if (modified) {
            this.sizeModified = true;
        }
        return modified;
    }
    
    @Override
    public void clear() {
        // This will cause the MultiSetBackedSortedSet to call clear on each Set in its Set of Sets, including the buffer
        // It will also call clear on its Set of Sets, emptying the contents
        set.clear();
        // Null the buffer so that it will start new on the next add
        buffer = null;
        this.size = 0;
        this.sizeModified = false;
    }
    
    @Override
    public Comparator<? super E> comparator() {
        return comparator;
    }
    
    @Override
    public SortedSet<E> subSet(E fromElement, E toElement) {
        return set.subSet(fromElement, toElement);
    }
    
    @Override
    public SortedSet<E> headSet(E toElement) {
        return set.headSet(toElement);
    }
    
    @Override
    public SortedSet<E> tailSet(E fromElement) {
        return set.tailSet(fromElement);
    }
    
    @Override
    public E first() {
        return set.first();
    }
    
    @Override
    public E last() {
        return set.last();
    }
}
