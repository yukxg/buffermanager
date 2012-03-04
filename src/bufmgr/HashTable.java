package bufmgr;

import java.io.ObjectInputStream.GetField;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;

public class HashTable<k, v> {
	private int capacity;
	private int scale;
	private int shift;
	private int n;
	private LinkedList<Entry<k, v>> a[];

	public HashTable() {
		capacity = 1023;
		Random r = new Random();
		scale = r.nextInt(capacity - 1) + 1;
		
		shift = r.nextInt(capacity);
	//	System.out.println(scale+" "+shift);
		a = (LinkedList<Entry<k, v>>[]) new LinkedList[1024];
		for(int i=0;i<a.length;i++)
			a[i]=new LinkedList<Entry<k,v>>();
		n=0;
	}
	
	public boolean conatin(k Key)
	{
		return get(Key)==null? false:true;
	}
	public void put(k key,v value)
	{
		int i=hashValue(key);
		Iterator <Entry<k, v>>it=a[i].iterator();
		Entry<k, v>e=null;
		boolean found=false; 
		while(it.hasNext()&&!found)
		{
			e=it.next();
			if(e.getKey()==key)
			{
				found=true;
				e.setValue(value);
			}
		}
		if(!found)
		{
			a[i].push(new Entry(key, value));
		}
	}
	
	private int hashValue(k key)
	{
		return Math.abs((scale*key.hashCode()+shift)%capacity);
	}
	
	public v get(k key){
		int i=hashValue(key);
		Iterator <Entry<k, v>>it=a[i].iterator();
		
		Entry<k, v>e=null;
		while(it.hasNext())
		{
			e=it.next();
			
			if(e.getKey().hashCode()==key.hashCode()){
			
				return e.getValue();
			}
		}
		return null;
	}
	
//	private int getIndex(k key)
//	{
//		int i=hashValue(key);
//		int temp=-1;
//		int counter=0;
//		boolean found=false;
//		while(a[i]!=null&&counter<capacity)
//		{
//			counter++;
//			if(a[i].isEmpty())
//				temp=i;	
//			else if(key.equals(a[i].getKey()))
//			{
//				found=true;
//				temp=-1;
//				break;
//			}
//			i=(i+1)%capacity;
//		}
//		if(temp!=-1)
//			i=temp;
//		if(found)
//			return i;
//		else
//			return -1*i;
//	}
	public void remove(k key){
		int i=hashValue(key);
		Iterator <Entry<k, v>>it=a[i].iterator();
		Entry<k, v>e=null;
		int count=-1;
		while(it.hasNext()&&count!=-2)
		{
			count++;
			e=it.next();
			if(e.getKey().hashCode()==key.hashCode())
			{
				a[i].remove(count);
				count=-2;
			}
			
		}
	}
	public String toString()
	{
		int count=0;
		for(int i=0;i<capacity;i++)
			if(a[i]!=null&&!a[i].isEmpty())
				count++;
		System.out.println(count);
		
		return Arrays.toString(a);
		
	}
	public static void main(String[] args) {
		HashTable<Integer, Integer> ht=new HashTable<Integer, Integer>();
		ht.put(50, 10);
		ht.put(51, 20);
		ht.put(50, 50);
	
		ht.put(53, 10);
		ht.put(512, 20);
		ht.put(5123123, 50);
		
		ht.put(21, 20);
		ht.put(20, 50);
		ht.put(23, 10);
		ht.put(5212, 20);
		ht.put(2123123, 50);
		ht.put(121, 20);
		ht.put(30, 50);
		ht.put(13, 10);
		ht.put(1212, 20);
		ht.put(323123, 50);
		
		ht.put(1211, 20);
		ht.put(302, 50);
		ht.put(133, 10);
		ht.put(12132, 20);
		ht.put(3231523, 50);
		ht.put(323152, 50);
		ht.remove(133);
		System.out.println(ht.get(133));
		ht.put(32315, 50);
		System.out.println(ht);
		System.out.println(ht.get(32315));
		//System.out.println(ht);
	
	}

}
