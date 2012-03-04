package bufmgr;

public class Entry<k,v>{
	protected k key;
	protected v value;
	protected boolean avilable;
	public Entry(k k,v v)
	{
		key=k;
		value=v;
		avilable =false;
	}
	public void setKey(k key) {
		this.key = key;
	}
	public k getKey() {
		return key;
	}
	public void setValue(v value) {
		this.value = value;
	}
	public v getValue() {
		return value;
	}
	public void setAvilable(boolean avilable) {
		this.avilable = avilable;
	}
	public boolean isAvilable(){
		return avilable;
	}
	public boolean isEmpty(){
		return key==null&&value==null;
	}
	public String toString(){
		if(isEmpty())
			return null;
		return key.toString()+" "+value.toString();
	}
	public void clear() {
		key=null;
		value=null;
		
	}
}