package model;

import java.util.Comparator;

public class DESCTaskComparator implements Comparator<Object>{
	public int compare(Object o1, Object o2)
    {
         Task n1 = (Task)o1;
         Task n2 = (Task)o2;
         if(n1.getProcessTime() + n1.getInputSize() / Cluster.LOCAL_RATE < n2.getProcessTime() + n2.getInputSize() / Cluster.LOCAL_RATE) 
        	 return 1;
         else if(n1.getProcessTime() + n1.getInputSize() / Cluster.LOCAL_RATE == n2.getProcessTime() + n2.getInputSize() / Cluster.LOCAL_RATE) 
        	 return 0;
         return -1;
    }
}
