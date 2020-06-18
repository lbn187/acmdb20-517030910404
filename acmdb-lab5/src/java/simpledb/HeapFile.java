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
	private File file;
	private TupleDesc tupledesc;
	public class HeapFileIterator implements DbFileIterator{
		/**
		 * Opens the iterator
		 * @throws DbException when there are problems opening/accessing the database.
		 */
		private HeapFile file;
		private TransactionId tid;
		private int pid;
		private Iterator<Tuple> tupleit;
		public HeapFileIterator(HeapFile f, TransactionId t){
			file = f;
			tid = t;
		}
		@Override
		public void open()
			throws DbException, TransactionAbortedException{
			pid = 0;
			if(file.numPages() == 0) tupleit = new ArrayList<Tuple>().iterator();
			else
			tupleit = ((HeapPage)Database.getBufferPool().getPage(tid,new HeapPageId(file.getId(), pid), Permissions.READ_ONLY)).iterator();
		}

		/** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
		@Override
		public boolean hasNext()
			throws DbException, TransactionAbortedException{
				if(tupleit == null)return false;
				if(tupleit.hasNext())return true;
				if(pid >= file.numPages() - 1)return false;
				return ((HeapPage)Database.getBufferPool().getPage(tid,new HeapPageId(file.getId(), pid + 1),Permissions.READ_ONLY)).iterator().hasNext();
		}

		/**
		 * Gets the next tuple from the operator (typically implementing by reading
		 * from a child operator or an access method).
		 *
		 * @return The next tuple in the iterator.
		 * @throws NoSuchElementException if there are no more tuples
		 */
		@Override
		public Tuple next()
			throws DbException, TransactionAbortedException, NoSuchElementException{
			if(tupleit == null) throw new NoSuchElementException();
			for(;!tupleit.hasNext();
			tupleit = ((HeapPage)Database.getBufferPool().getPage(tid,new HeapPageId(file.getId(), ++pid),Permissions.READ_ONLY)).iterator()
			){
				if(pid >= file.numPages() - 1) throw new NoSuchElementException();
			}
			return tupleit.next();
		}

		/**
		 * Resets the iterator to the start.
		 * @throws DbException When rewind is unsupported.
		 */
		@Override
		public void rewind() throws DbException, TransactionAbortedException{
			close();
			open();
		}

		/**
		 * Closes the iterator.
		 */
		@Override
		public void close(){
			tupleit = null;
		}
	}
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
		file = f;
		tupledesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
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
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupledesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
		try{
			/*FileInputStream stream = new FileInputStream(file);
			stream.skip(pid.pageNumber() * BufferPool.getPageSize());
			byte[] bs = new byte[BufferPool.getPageSize()];
			stream.read(bs);
			stream.close();
			return new HeapPage((HeapPageId)pid, bs);*/
			RandomAccessFile stream = new RandomAccessFile(file, "r");
			byte[] bs = new byte[BufferPool.getPageSize()];
			stream.seek(pid.pageNumber() * BufferPool.getPageSize());
			stream.read(bs);
			stream.close();
			return new HeapPage((HeapPageId)pid, bs);
		} catch(IOException e){
			e.printStackTrace();
		}
		throw new IllegalArgumentException();
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
		RandomAccessFile stream = new RandomAccessFile(file, "rw");
		PageId pid = page.getId();
		byte[] bs = page.getPageData();
		stream.seek(pid.pageNumber() * BufferPool.getPageSize());
		stream.write(bs, 0, BufferPool.getPageSize());
		stream.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)(1.0 * file.length() / BufferPool.getPageSize() + 0.5);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> dirtypages = new ArrayList<>();
		for(int i = 0; i < numPages(); i++){
			HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
			if(page.getNumEmptySlots() > 0){
				page.insertTuple(t);
				dirtypages.add(page);
				return dirtypages;
			}
		}
		HeapPage page = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());
		page.insertTuple(t);
		dirtypages.add(page);
		writePage(page);
		return dirtypages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
		page.deleteTuple(t);
		ArrayList<Page> dirtypages = new ArrayList<>();
		dirtypages.add(page);
		return dirtypages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

}

