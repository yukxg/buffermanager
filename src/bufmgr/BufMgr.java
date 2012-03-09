package bufmgr;

import java.io.IOException;
import java.util.Iterator;
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
import diskmgr.PageUnpinnedException;

public class BufMgr {
	private Page[] bufPool;
	private descriptors[] bufDescr;
	private Queue<PageId> queue;
	private int numOfPage;
	private int top;
	private int numbufs;
	private HashTable<Integer, Integer> hash;
	private DB db;

	public BufMgr(int numbufs, String replacerArg) {
		this.numbufs = numbufs;
		bufPool = new Page[numbufs];
		bufDescr = new descriptors[numbufs];
		numOfPage = 0;
		db = new DB();
		try {
			db.openDB("name", numbufs);
		} catch (Exception e) {
			// Do Nothing !
		}
		hash = new HashTable<Integer, Integer>();
		if (replacerArg.charAt(0) == 'L')
			queue = new LinkedList<PageId>();
		else {
			queue = new LinkedList<PageId>();
		}
	}

	public int getFirstEmptyFrame() throws BufferPoolExceededException,
			HashEntryNotFoundException, FreePageException {
		if (isFull()) {
			if (queue.size() == 0)
				throw new BufferPoolExceededException(null,
						"BUFMGR: NO_EMPTY_FRAME");
			else {
				PageId id = new PageId();
				id = queue.poll();
				int frameNumber = 0;
				try {
					frameNumber = getFrameNumber(id);
				} catch (Exception e) {
					throw new HashEntryNotFoundException(null,
							"BUFMGR: NO_EMPTY_FRAME");
				}
				try {
					freePage(id);
				} catch (Exception e) {
					throw new FreePageException(null, "BUFMGR: NO_EMPTY_FRAME");
				}
				return frameNumber;
			}
		} else {
			// int i = 0;
			// if (top >= numbufs)
			// while (i < numbufs && bufPool[i++] != null)
			// ;
			// else if (top < numbufs)
			// i = top;
			// return i;
			int index = -1;
			for (int i = 0; i < numbufs; i++) {
				if (bufPool[i] == null) {
					index = i;
					break;
				}
			}
			return index;
		}
	}

	public boolean isFull() {
		return (numOfPage == numbufs) ? true : false;
	}

	private int getFrameNumber(PageId pId) throws HashEntryNotFoundException {
		if (hash.conatin(pId.pid))
			return hash.get(pId.pid);
		else {
			throw new HashEntryNotFoundException(null,
					"BUF_MNGR:HASH_ENTRY_NOT_FOUND_EXCEPTION");
		}
	}

	// public void pinPage(PageId pageno, Page page, boolean emptyPage)
	// throws DiskMgrException, BufferPoolExceededException,
	// PagePinnedException {
	// boolean found;
	// if(pageno.pid==2)
	// {
	// System.out.println();
	// }
	// found = hash.conatin(pageno.pid);
	// if (found) {
	// int index = hash.get(pageno.pid);
	// if (bufDescr[index].getPin_count() == 0) {
	// queue.remove(page);
	// bufDescr[index]
	// .setPin_count(bufDescr[index].getPin_count() + 1);
	// bufPool[index]=page;
	// } else {
	// bufDescr[index]
	// .setPin_count(bufDescr[index].getPin_count() + 1);
	// bufPool[index]=page;
	// }
	// } else {
	// if (isFull()) {
	// if (queue.size() == 0) {
	// throw new BufferPoolExceededException(null,
	// "BUFMGR:PAGE_PIN_FAILED");
	// } else {
	// PageId id = queue.poll();
	// int index = hash.get(id.pid);
	// if (bufDescr[index].isDirtyBit()) {
	// // write this first
	// try {
	// db.write_page(pageno, bufPool[index]);
	// } catch (Exception e) {
	// throw new DiskMgrException(e,
	// "DB.java: pinPage() failed");
	// }
	// }
	// hash.remove(bufDescr[index].getPageNumber().pid);
	// // make DB read this page
	// try {
	// db.read_page(pageno, page);
	// } catch (Exception e) {
	// throw new DiskMgrException(e,
	// "DB.java: pinPage() failed");
	// }
	// // put this page at index
	// // construct its descriptors and put them at index
	// bufPool[index] = page;
	// bufDescr[index] = new descriptors(1, pageno, false);
	// hash.put(pageno.pid, index);
	// }
	// } else {
	// // I must read this page first
	// try {
	// // db.openDB("name");
	// db.read_page(pageno, page);
	// } catch (Exception e) {
	// throw new DiskMgrException(e, "DB.java: pinPage() failed");
	// }
	// int index = -1;
	// try {
	// index = getFirstEmptyFrame();
	// } catch (Exception e) {
	// throw new PagePinnedException(null,
	// "BUFMGR:PAGE_PIN_FAILED");
	// }
	// bufPool[index] = page;
	// bufDescr[index] = new descriptors(1, pageno, false);
	// hash.put(pageno.pid, index);
	// numOfPage++;
	// top++;
	// }
	// }
	//
	// }

		public void pinPage(PageId pageno, Page page, boolean emptyPage)
			throws DiskMgrException, BufferPoolExceededException,
			PagePinnedException, InvalidPageNumberException, FileIOException,
			IOException, HashEntryNotFoundException {
		boolean found;
		
		found = hash.conatin(pageno.pid);
		if (found) {
			int index = hash.get(pageno.pid);
			if (emptyPage == true) {
				bufPool[index] = new Page(page.getpage().clone());
				page.setpage(bufPool[index].getpage());
				db.write_page(pageno, page);
			} else {
				page.setpage(bufPool[index].getpage());
			}
			if (bufDescr[index].getPin_count() == 0) {
				queue.remove(pageno);
				bufDescr[index]
						.setPin_count(bufDescr[index].getPin_count() + 1);
			} else {
				bufDescr[index]
						.setPin_count(bufDescr[index].getPin_count() + 1);
			}

		} else {
			if (isFull()) {
				if (queue.size() == 0) {
					throw new BufferPoolExceededException(null,
							"BUFMGR:PAGE_PIN_FAILED");
				} else {
					PageId id=new PageId();
					id=queue.poll();
					int index = hash.get(id.pid);
					if (bufDescr[index].isDirtyBit()) {
						// write this first
						flushPage(id);
					}
					hash.remove(bufDescr[index].getPageNumber().pid);
					// make DB read this page
					try {
						db.read_page(pageno, page);
					} catch (Exception e) {
						throw new DiskMgrException(e,
								"DB.java: pinPage() failed");
					}
					// put this page at index
					// construct its descriptors and put them at index
					bufPool[index] = page;
					bufDescr[index] = new descriptors(1, pageno, false);
					hash.put(pageno.pid, index);
				}
			} else {
				// I must read this page first
				Page p = new Page();
				try {
					// db.openDB("name");

					db.read_page(pageno, p);
				} catch (Exception e) {
					throw new DiskMgrException(e, "DB.java: pinPage() failed");
				}
				int index = -1;
				try {
					index = getFirstEmptyFrame();
				} catch (Exception e) {
					throw new PagePinnedException(null,
							"BUFMGR:PAGE_PIN_FAILED");
				}
				bufPool[index] = p;
				bufDescr[index] = new descriptors(1, pageno, false);
				hash.put(pageno.pid, index);
				page.setpage(bufPool[index].getpage());
				db.write_page(pageno, page);
				numOfPage++;
			}
			checkMehtod();
		}
//		System.out.println(hash);
//		System.out.println(0 + " " + bufPool[getFrameNumber(new PageId(0))]);
//		System.out.println(1 + " " + bufPool[getFrameNumber(new PageId(1))]);
//		System.out.println(2 + " " + bufPool[getFrameNumber(new PageId(2))]);
//		System.out.println(3 + " " + bufPool[getFrameNumber(new PageId(3))]);
	}
		int counter=0;
		public void checkMehtod()
		{
//			counter++;
//			if(counter==4)
//				System.out.println();
//			for(int i=0;i<bufDescr.length;i++)
//			{
//				if(bufDescr[i]!=null&&bufDescr[i].getPin_count()==0&&queue.contains(bufDescr[i].getPageNumber())==false)
//					System.out.println("Error here "+bufPool[i]+" "+i+" "+counter);
//			}
//			System.out.print(' ');
		}

	public void unpinPage(PageId pageno, boolean dirty)
			throws PageUnpinnedException, HashEntryNotFoundException {
		if (hash.conatin(pageno.pid)) {
			int index = hash.get(pageno.pid);
			if (bufDescr[index].getPin_count() == 0) {
				throw new PageUnpinnedException(null,
						"BUFMGR:PAGE_UNPIN_FAILED");
			} else {
				bufDescr[index].setDirtyBit(dirty);
				bufDescr[index]
						.setPin_count(bufDescr[index].getPin_count() - 1);
				if (!isInQueue(pageno)&&bufDescr[index].getPin_count() == 0)
					queue.add(pageno);

			}
		} else {
			throw new HashEntryNotFoundException(null,
					"BUFMGR:PAGE_UNPIN_FAILED");
		}
		checkMehtod();
	}
	public boolean isInQueue(PageId id)
	{
		Iterator<PageId> myIterator= queue.iterator();
		while(myIterator.hasNext())
		{
			if(myIterator.next().pid==id.pid)
				return true;
		}
		return false;
	}

	public PageId newPage(Page firstpage, int howmany) throws DiskMgrException,
			FreePageException {
		int i = -1;
		try {
			i = getFirstEmptyFrame();
		} catch (Exception e) {
			throw new FreePageException(null, "BUFMGR:FAIL_NEW_PAGE.");
		}
		bufPool[i] = firstpage;
		PageId id = new PageId();
		try {
			db.allocate_page(id, howmany);
		} catch (Exception e) {
			throw new DiskMgrException(e, "DB.java: newPage() failed");
		}
		descriptors des = new descriptors(1, id, false);// here the pin count
		bufDescr[i] = des;
		hash.put(id.pid, i);
		top++;
		numOfPage++;
		checkMehtod();
		return id;
	}

	public void freePage(PageId globalPageId) throws FreePageException {

		if (hash.conatin(globalPageId.pid)) {
			int i;
			try {
				i = getFrameNumber(globalPageId);
				if (bufDescr[i].getPin_count() > 0) {
					throw new PagePinnedException(null,
							"DB.java: freePage() failed");
				}
				if (bufDescr[i].isDirtyBit())
					try {
						flushPage(globalPageId);
					} catch (Exception e) {
						throw new FreePageException(null,
								"BUFMGR: FAIL_PAGE_FREE");
					}
				hash.remove(globalPageId.pid);
				numOfPage--;
				bufPool[i] = null;
				bufDescr[i] = null;
			} catch (Exception e) {
				throw new FreePageException(null, "BUFMGR:FAIL_PAGE_FREE");
			}

		} else
			throw new FreePageException(null, "BUFMGR:FAIL_PAGE_FREE");
		checkMehtod();
	}

	public void flushPage(PageId pageid) throws HashEntryNotFoundException,
			DiskMgrException {
		Page apage = null;
		for (int i = 0; i < bufDescr.length; i++) {
			if (bufDescr[i] != null
					&& bufDescr[i].getPageNumber().pid == (pageid.pid)) {
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

	public void flushAllPages() throws HashEntryNotFoundException,
			DiskMgrException {
		for (int i = 0; i < numbufs; i++) {
			if ((bufDescr[i] != null))
				flushPage(bufDescr[i].getPageNumber());
		}
	}

	public int getNumUnpinnedBuffers() {
		return queue.size();
	}
}
