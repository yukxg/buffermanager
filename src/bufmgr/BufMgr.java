package bufmgr;

import global.PageId;
import diskmgr.Page;

public class BufMgr {
	public BufMgr(int numbufs, String replacerArg) {

	}

	public void pinPage(PageId pageno, Page page, boolean emptyPage) {
		// TODO Auto-generated method stub
		
	}

	public void unpinPage(PageId pageno, boolean dirty) {
		// TODO Auto-generated method stub
		
	}
	
	public PageId newPage(Page firstpage,int homany){
		return null;
		
	}
	
	public void freePage(PageId globalPageId)
	{
		
	}
	
	public void flushPage(PageId pageid){
		
	}

}
