package bufmgr;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import chainexception.ChainException;
import global.PageId;
import diskmgr.DB;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import diskmgr.Page;

public class BufMgr {
	static Page[] bufPool;
	static descriptors[] bufDescr;
	Queue<Page> queue;
	int numOfPage;
	DB db;

	public BufMgr(int numbufs, String replacerArg) {
		// multiple object access to the buffer pool !
		bufPool = new Page[numbufs];
		bufDescr = new descriptors[numbufs];
		numOfPage = 0;
		db = new DB();
		if (replacerArg.charAt(0) == 'L')
			queue = new LinkedList<Page>();
		else {
			// Other arrangement for policy
		}
	}

	public void pinPage(PageId pageno, Page page, boolean emptyPage)
			throws ChainException {
		// TODO Auto-generated method stub

	}

	public void unpinPage(PageId pageno, boolean dirty) throws ChainException {
		// TODO Auto-generated method stub

	}

	public PageId newPage(Page firstpage, int homany) {
		return null;

	}

	public void freePage(PageId globalPageId) throws ChainException {

	}

	public void flushPage(PageId pageid) throws InvalidPageNumberException,
			FileIOException, IOException {
		Page apage = null;
		for (int i = 0; i < bufDescr.length; i++) {
			if ((bufDescr[i].getPageNumber()).equals(pageid)) {
				apage = bufPool[i];
				break;
			}
		}
		if (apage != null)
			db.write_page(pageid, apage);
		else {
			// TODO handle the exception part!
			// throw new InvalidPageNumberException(ex, name);
		}
	}

	public int getNumUnpinnedBuffers() {
		// TODO Auto-generated method stub
		return 0;
	}

}
