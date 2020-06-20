package simpledb;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    private class Lock{
        private Set<TransactionId> sharedLock = ConcurrentHashMap.newKeySet();
        private TransactionId exclusiveLock = null;
        boolean lockJudge(Permissions perm, TransactionId tid){
            if(perm.equals(Permissions.READ_ONLY)){
				if(exclusiveLock != null)return exclusiveLock.equals(tid);
				sharedLock.add(tid);
				return true;
			}else{
				if(exclusiveLock != null)return exclusiveLock.equals(tid);
				if(sharedLock.size() > 1)return false;
				if(sharedLock.size() == 0 || sharedLock.contains(tid)){
					exclusiveLock = tid;
					sharedLock.clear();
					return true;
				}
				return false;
			}
        }
        void releaseLock(TransactionId tid){
			if(tid.equals(exclusiveLock))exclusiveLock = null;
			else sharedLock.remove(tid);
        }
        boolean holdsLock(TransactionId tid){
            return tid.equals(exclusiveLock) || sharedLock.contains(tid);
        }
        Set<TransactionId> alllock(){
            Set<TransactionId> tids= new HashSet<>(sharedLock);
            if(exclusiveLock != null)tids.add(exclusiveLock);
            return tids;
        }
    }

    private class LockGraph{
        private ConcurrentHashMap<TransactionId, HashSet<TransactionId>> edges = new ConcurrentHashMap<>();
        synchronized void update(TransactionId tid, PageId pid){
            edges.putIfAbsent(tid, new HashSet<>());
            HashSet<TransactionId> tidedges = edges.get(tid);
            tidedges.clear();
            if (pid == null) return;
            Set<TransactionId> tids = new HashSet<>();
			Lock lock = lockMap.get(pid);
            synchronized (lock){
				tids.addAll(lock.sharedLock);
				if(lock.exclusiveLock != null)tids.add(lock.exclusiveLock);
            }
            tidedges.addAll(tids);
        }
        synchronized boolean deadlockJudge(TransactionId s){
            Queue<TransactionId> queue = new LinkedList<>();
			HashSet<TransactionId> vis = new HashSet<>();
            queue.add(s);
            while(!queue.isEmpty()){
                TransactionId tidx = queue.poll();
                HashSet<TransactionId> tidset = edges.get(tidx);
                for(TransactionId tidy:tidset){
					if(!vis.contains(tidy)){
						queue.add(tidy);
						vis.add(tidy);
					}
					if(tidy.equals(s))return true;
                }
            }
            return false;
        }
    }
	private final int numPages;
    private ConcurrentHashMap<PageId, Page> pageMap;
    private ConcurrentHashMap<PageId, Lock> lockMap;
    private ConcurrentHashMap<TransactionId, Set<PageId>> tidMap;
    private LockGraph lockgraph;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        pageMap = new ConcurrentHashMap<>();
        lockMap = new ConcurrentHashMap<>();
        tidMap = new ConcurrentHashMap<>();
        lockgraph = new LockGraph();
    }
	
    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        lockMap.putIfAbsent(pid, new Lock());
        boolean flag;
        synchronized(lockMap.get(pid)){
            flag = lockMap.get(pid).lockJudge(perm, tid);
        }
        while(!flag){
            Thread.yield();
            lockgraph.update(tid, pid);
            if (lockgraph.deadlockJudge(tid)) {
                throw new TransactionAbortedException();
            }
            Thread.yield();
            synchronized(lockMap.get(pid)){
                flag = lockMap.get(pid).lockJudge(perm, tid);
            }
        }
        lockgraph.update(tid, null);
        tidMap.putIfAbsent(tid, new HashSet<>());
        tidMap.get(tid).add(pid);
		if(pageMap.get(pid) != null)return pageMap.get(pid);
		while(pageMap.size() >= numPages)
			evictPage();
		Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
		pageMap.put(pid, page);
		page.setBeforeImage();
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        synchronized(lockMap.get(pid)){
            lockMap.get(pid).releaseLock(tid);
        }
        tidMap.get(tid).remove(pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        boolean flag = false;
        synchronized(lockMap.get(p)){
            flag = lockMap.get(p).holdsLock(tid);
        }
        return flag;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Set<PageId> pages = tidMap.get(tid);
        tidMap.remove(tid);
        if(pages == null)return;
        for(PageId pid:pages){
            Page page = pageMap.get(pid);
            if(page != null && lockMap.get(pid).exclusiveLock != null){
				if(commit){
					flushPage(pid);
					page.setBeforeImage();
				}else{
					pageMap.put(pid, page.getBeforeImage());
				}
            }
            synchronized(lockMap.get(pid)){
                lockMap.get(pid).releaseLock(tid);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page page:pages){
            PageId pid = page.getId();
            while(!pageMap.containsKey(pid) && pageMap.size() >= numPages)
                evictPage();
            page.markDirty(true, tid);
            pageMap.put(pid, page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        ArrayList<Page> pages = Database.getCatalog().getDatabaseFile(tableId).deleteTuple(tid, t);
        for(Page page:pages){
            PageId pid = page.getId();
            while(!pageMap.containsKey(pid) && pageMap.size() >= numPages)
                evictPage();
            page.markDirty(true, tid);
            pageMap.put(pid, page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(PageId pid:pageMap.keySet())
            flushPage(pid);
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pageMap.get(pid);
        if(page == null) throw new IOException();
        if(page.isDirty() == null)return;
        page.markDirty(false, null);
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        for(PageId pid:pageMap.keySet()){
            synchronized (pageMap.get(pid)){
                if(pageMap.get(pid).isDirty() == null){
                    try {
						flushPage(pid);
					} catch (IOException e) {
						e.printStackTrace();
					}
					pageMap.remove(pid);
                    return;
                }
            }
        }
        throw new DbException("None page can be evicted for NO STEAL POLICY!");
    }
}
