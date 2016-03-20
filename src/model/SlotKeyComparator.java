package model;

import java.util.Comparator;

public class SlotKeyComparator implements Comparator<Object>{

	@Override
	public int compare(Object o1, Object o2) {
		DataNode j1 = (DataNode)o1;
		DataNode j2 = (DataNode)o2;
        if(j1.getSortKey() < j2.getSortKey()) 
       	 return 1;
        else if( j1.getSortKey() == j2.getSortKey()) 
       	 return 0;
        return -1;
	}

}
