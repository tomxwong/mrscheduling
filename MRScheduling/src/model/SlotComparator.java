package model;

import java.util.Comparator;

public class SlotComparator implements Comparator<Object>{

	@Override
	public int compare(Object o1, Object o2) {
		DataNode n1 = (DataNode)o1;
        DataNode n2 = (DataNode)o2;
        if(n1.getCurFinishTime() < n2.getCurFinishTime()) 
       	 return -1;
        else if(n1.getCurFinishTime() == n2.getCurFinishTime()) 
       	 return 0;
        return 1;
	}

}
