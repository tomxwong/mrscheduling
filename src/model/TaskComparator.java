package model;

import java.util.Comparator;

public class TaskComparator implements Comparator<Object>{

	@Override
	public int compare(Object o1, Object o2) {
		Task j1 = (Task)o1;
		Task j2 = (Task)o2;
        if(j1.getSortKey() < j2.getSortKey()) 
       	 return -1;
        else if( j1.getSortKey() == j2.getSortKey()) 
       	 return 0;
        return 1;
	}

}
