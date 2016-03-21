package model;

import java.util.Comparator;

public class JobComparator implements Comparator<Object>{

	@Override
	public int compare(Object o1, Object o2) {
		Job j1 = (Job)o1;
        Job j2 = (Job)o2;
        if(j1.getSortKey() > j2.getSortKey()) 
       	 return 1;
        else if( j1.getSortKey() == j2.getSortKey()) 
       	 return 0;
        return -1;
	}

}
