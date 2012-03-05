package bufmgr;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import chainexception.ChainException;
import global.PageId;
import diskmgr.DB;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import diskmgr.InvalidRunSizeException;
import diskmgr.OutOfSpaceException;
import diskmgr.Page;

public class BufMgr {


	static Page[] bufPool;
	static descriptors[] bufDescr;
	Queue<PageId> queue;
	int numOfPage;
	static int top;
	static int numbufs;
	HashTable<PageId, Integer> hash;
	DB db;

	
		

	public BufMgr(int numbufs, String replacerArg) {
		// multiple object access to the buffer pool !
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
	
	public int getFitstEmptyFrame()
	{
		int i=0;
		if(top>=numbufs)
			while(i<numbufs&&bufPool[i++]!=null);
		else if(top<numbufs)
			i=top;
		return i;
	}
	
	public boolean isFull()
	{
		return (numOfPage==numbufs )? true:false;
	}
	
	private int getFrameNumber(PageId pId)
	{
		if(hash.conatin(pId))
		{
			return hash.get(pId);
		}
		return -1;
	}
	
	public void pinPage(PageId pageno, Page page, boolean emptyPage) throws ChainException {
		boolean found;
		found = hash.conatin(pageno);
		if(found){
			int index = hash.get(pageno);
			if(bufDescr[index].getPin_count() == 0){
				queue.remove(page);
				bufDescr[index].setPin_count(bufDescr[index].getPin_count()+1);
			}else{
				bufDescr[index].setPin_count(bufDescr[index].getPin_count()+1);
			}
		}else{
			if(isFull()){
				if(queue.size() == 0){
					// throw exception
				}else{
					PageId id = queue.poll();
					int index = hash.get(id);
					if(bufDescr[index].isDirtyBit()){
						//write this first
						try {
							db.write_page(pageno, bufPool[index]);
						} catch (IOException e) {
			
						}
					}
					hash.remove(bufDescr[index].getPageNumber());
					// make DB read this page
					try {
						db.read_page(pageno, page);
					} catch (IOException e) {
						
					}
					//put this page at index
					// construct its descriptors and put them at index
					bufPool[index] = page;
					bufDescr[index] = new descriptors(1, pageno, false);
					hash.put(pageno, index);
				}
			}else{
				// I must read this page first
				try {
					db.read_page(pageno, page);
				} catch (IOException e) {
					
				}
				int index = getFitstEmptyFrame();
				bufPool[index] = page;
				bufDescr[index] = new descriptors(1,pageno, false);
				hash.put(pageno, index);
				numOfPage++;
				top++;
			}
		}
		
			

	}

	public void unpinPage(PageId pageno, boolean dirty) throws ChainException {
			if(hash.conatin(pageno)){
				int index = hash.get(pageno);
				if(bufDescr[index].getPin_count()==0){
					//throw exception
				}else{
					bufDescr[index].setDirtyBit(dirty);
					bufDescr[index].setPin_count(bufDescr[index].getPin_count()-1);
					if(bufDescr[index].getPin_count()==0)
						queue.add(pageno);
					
				}
			}else{
				//throw exception
			}
				

	}

	public PageId newPage(Page firstpage, int howmany) throws OutOfSpaceException, InvalidRunSizeException, InvalidPageNumberException, FileIOException, DiskMgrException, IOException {
		int i=getFitstEmptyFrame();//here will return zero in case the pool is full and the pin id
		bufPool[i]=firstpage;
		PageId id=new PageId(i);
		descriptors des=new descriptors(1, id, false);// here the pin count
		bufDescr[i]=des;
		hash.put(id, i);
		db.allocate_page(id, howmany);
		top++;
		numOfPage++;
		
		return id;
	}
	
	public void freePage(PageId globalPageId)throws ChainException, IOException {
	
		if(hash.conatin(globalPageId))
		{
			int i=getFrameNumber(globalPageId);
			if(bufDescr[i].isDirtyBit())
				flushPage(globalPageId);
			hash.remove(globalPageId);
			bufPool[i]=null;
			bufDescr[i]=null;
		}
		
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
