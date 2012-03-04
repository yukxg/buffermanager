package bufmgr;

import chainexception.ChainException;
import global.PageId;
import diskmgr.Page;

public class BufMgr {
	public BufMgr(int numbufs, String replacerArg) {

	}

	public void pinPage(PageId pageno, Page page, boolean emptyPage)throws ChainException {
		// TODO Auto-generated method stub
		
	}

	public void unpinPage(PageId pageno, boolean dirty)throws ChainException {
		// TODO Auto-generated method stub
		
	}
	
	public PageId newPage(Page firstpage,int homany){
		return null;
		
	}
	
	public void freePage(PageId globalPageId)throws ChainException
	{
		
	}
	
	public void flushPage(PageId pageid){
		
	}

	public int getNumUnpinnedBuffers() {
		// TODO Auto-generated method stub
		return 0;
	}


}
