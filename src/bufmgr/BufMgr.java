package bufmgr;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import chainexception.ChainException;
import global.PageId;
import diskmgr.BufferPoolExceededException;
import diskmgr.DB;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.FreePageException;
import diskmgr.HashEntryNotFoundException;
import diskmgr.InvalidPageNumberException;
import diskmgr.InvalidRunSizeException;
import diskmgr.OutOfSpaceException;
import diskmgr.Page;
import diskmgr.PagePinnedException;

public class BufMgr {

	private Page[] bufPool;
	private descriptors[] bufDescr;
	private Queue<PageId> queue;
	private int numOfPage;
	private int top;
	private int numbufs;
	private HashTable<PageId, Integer> hash;
	private DB db;

	public BufMgr(int numbufs, String replacerArg) {
		bufPool = new Page[numbufs];
		bufDescr = new descriptors[numbufs];
		numOfPage = 0;
		db = new DB();
		if (replacerArg.charAt(0) == 'L')
			queue = new LinkedList<PageId>();
		else {
			// Other arrangement for policy
		}

	}

	public int getFitstEmptyFrame() {
		
		int i = 0;
		if (top >= numbufs)
			while (i < numbufs && bufPool[i++] != null)
				;
		else if (top < numbufs)
			i = top;
		return i;
	}

	public boolean isFull() {
		return (numOfPage == numbufs) ? true : false;
	}

	private int getFrameNumber(PageId pId) throws HashEntryNotFoundException {
			if (hash.conatin(pId)) 
				return hash.get(pId);
			else{
			throw new HashEntryNotFoundException(null,"BUF_MNGR:HASH ENTRY NOT FOUND EXCEPTION");
			}
	}

	public void pinPage(PageId pageno, Page page, boolean emptyPage) throws DiskMgrException, BufferPoolExceededException{
		boolean found;
		found = hash.conatin(pageno);
		if (found) {
			int index = hash.get(pageno);
			if (bufDescr[index].getPin_count() == 0) {
				queue.remove(page);
				bufDescr[index]
						.setPin_count(bufDescr[index].getPin_count() + 1);
			} else {
				bufDescr[index]
						.setPin_count(bufDescr[index].getPin_count() + 1);
			}
		} else {
			if (isFull()) {
				if (queue.size() == 0) {
					throw new BufferPoolExceededException(null, "DB.java: pinPage() failed");
				} else {
					PageId id = queue.poll();
					int index = hash.get(id);
					if (bufDescr[index].isDirtyBit()) {
						// write this first
						try {
							db.write_page(pageno, bufPool[index]);
						} catch (Exception e) {
							throw new DiskMgrException(e, "DB.java: pinPage() failed");
						}
					}
					hash.remove(bufDescr[index].getPageNumber());
					// make DB read this page
					try {
						db.read_page(pageno, page);
					} catch (Exception e) {
						throw new DiskMgrException(e, "DB.java: pinPage() failed");
					}
					// put this page at index
					// construct its descriptors and put them at index
					bufPool[index] = page;
					bufDescr[index] = new descriptors(1, pageno, false);
					hash.put(pageno, index);
				}
			} else {
				// I must read this page first
				try {
					db.read_page(pageno, page);
				} catch (Exception e) {
					throw new DiskMgrException(e, "DB.java: pinPage() failed");
				}
				int index = getFitstEmptyFrame();
				bufPool[index] = page;
				bufDescr[index] = new descriptors(1, pageno, false);
				hash.put(pageno, index);
				numOfPage++;
				top++;
			}
		}

	}

	public void unpinPage(PageId pageno, boolean dirty) throws PagePinnedException, HashEntryNotFoundException  {
		if (hash.conatin(pageno)) {
			int index = hash.get(pageno);
			if (bufDescr[index].getPin_count() == 0) {
				throw new PagePinnedException(null, "DB.java: unpinPage() failed");
			} else {
				bufDescr[index].setDirtyBit(dirty);
				bufDescr[index]
						.setPin_count(bufDescr[index].getPin_count() - 1);
				if (bufDescr[index].getPin_count() == 0)
					queue.add(pageno);

			}
		} else {
			throw new HashEntryNotFoundException(null, "DB.java: unpinPage() failed");
		}

	}

	public PageId newPage(Page firstpage, int howmany) throws DiskMgrException{
		int i = getFitstEmptyFrame();// here will return zero in case the pool
										// is full and the pin id
		bufPool[i] = firstpage;
		PageId id = new PageId();
		descriptors des = new descriptors(1, id, false);// here the pin count
		bufDescr[i] = des;
		hash.put(id, i);
		try {
			db.allocate_page(id, howmany);
		} catch (Exception e) {
			throw new DiskMgrException(e, "DB.java: newPage() failed");
		}
		top++;
		numOfPage++;
		return id;
	}

	public void freePage(PageId globalPageId) throws FreePageException    {

		if (hash.conatin(globalPageId)) {
			int i;
			try {
				i = getFrameNumber(globalPageId);
			if (bufDescr[i].isDirtyBit())
				try {
					flushPage(globalPageId);
				} catch (Exception e) {
				throw new FreePageException(null,"BUFMGR: FAIL_PAGE_FREE");
				}
			hash.remove(globalPageId);
			bufPool[i] = null;
			bufDescr[i] = null;
			} catch (Exception e) {
				throw new FreePageException(null, "BUFMGR:FAIL_PAGE_FREE");
			}
		
		}else
			throw new FreePageException(null, "BUFMGR:FAIL_PAGE_FREE");

	}

	public void flushPage(PageId pageid) throws HashEntryNotFoundException,DiskMgrException {
		Page apage = null;
		for (int i = 0; i < bufDescr.length; i++) {
			if ((bufDescr[i].getPageNumber()).equals(pageid)) {
				apage = bufPool[i];
				break;
			}
		}
		try {
			if (apage != null)
				db.write_page(pageid, apage);
			else
				throw new HashEntryNotFoundException(null,
						"BUF_MNGR: PAGE NOT FLUSHED ID EXCEPTION!");
		} catch (Exception e) {
			throw new DiskMgrException(e, "DB.java: flushPage() failed");
		}
	}

	public int getNumUnpinnedBuffers() {
		// TODO Auto-generated method stub
		return 0;
	}

}
