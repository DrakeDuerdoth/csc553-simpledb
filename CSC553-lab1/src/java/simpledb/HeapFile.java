package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	private TupleDesc td;
	private int tableId;
	private int numPages;
	private File file;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
    	this.file = f;
    	this.tableId = f.getAbsoluteFile().hashCode();
    	this.numPages = (int) (f.length() / BufferPool.PAGE_SIZE);
    	this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
    	return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	int offset = BufferPool.PAGE_SIZE * pid.pageNumber();
    	byte[] data = new byte[BufferPool.PAGE_SIZE];
    	try {
    		RandomAccessFile raf = new RandomAccessFile(file,"r");
    		raf.seek(offset);
    		raf.read(data);
    		raf.close();
    		return new HeapPage((HeapPageId) pid,data);
    	}catch (FileNotFoundException e) {
    		e.printStackTrace();
   
    	}catch (IOException ioe) { ioe.printStackTrace();}
    	return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	
    	return this.numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	return new HeapFileIterator(tid,this.getId(),this.numPages());
   }
    
    public class HeapFileIterator implements DbFileIterator{

    	   private static final long serialVersionUID = 1L;
    	   TransactionId tid;
           int pageCounter, tableId, numPages;
           Page page;
           Iterator<Tuple> tuples;
           HeapPageId pid;
           
           
           public HeapFileIterator(TransactionId tid, int tableId, int numPages) {
               this.tid = tid;
               this.pageCounter = 0;
               this.tableId = tableId;
               this.numPages = numPages;
           }
           
           private Iterator<Tuple> getTuples(int pageNumber) throws  DbException, TransactionAbortedException {
               pid = new HeapPageId(tableId, pageNumber);
               HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
               return heapPage.iterator();
           }
    	
           public void open() throws DbException, TransactionAbortedException {
			   pageCounter = 0;
	           tuples = getTuples(pageCounter);	
           }

           public boolean hasNext() throws DbException, TransactionAbortedException {
	           if(tuples == null) return false;
	            if(tuples.hasNext()) return true;
	            if(pageCounter + 1 >= numPages) return false;
	 
	            while(pageCounter + 1 < numPages && !tuples.hasNext()){
	                pageCounter++;
	                tuples = getTuples(pageCounter);
	            }
	            return this.hasNext();
           }

           public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        	   if(tuples == null) throw new NoSuchElementException();
	           return tuples.next();
	        }

           public void rewind() throws DbException, TransactionAbortedException {
        	   close();
        	   open();
           }

           public void close() {
        	   tuples = null;
        	   pid = null;			
           }
    	
    }

}

