package data;

import java.util.ArrayList;

import model.DataNode;
import model.Job;
import model.Schedule;
import model.Slot;
import model.Task;

public class Tools {

	public static void clearSchedule(Schedule s) {
		for(Job job : s.getJobs())
		{
			job.setMapFinishTime(0);
			job.setReduceFinishTime(0);
			job.setStartTime(0);
			job.setFinishTime(0);
			job.setSortKey(0);
			
			for(Task map : job.getMaps())
			{
				map.setStartTime(0);
				map.setFinishTime(0);
				map.setSortKey(0);
				map.setProcessed(false);
			}
			
			for(Task reduce : job.getReduces())
			{
				reduce.setStartTime(0);
				reduce.setFinishTime(0);
				reduce.setSortKey(0);
				reduce.setProcessed(false);
			}
		}
		
		for(DataNode mn : s.getCluster().getMapNodes())
		{
			mn.setCurFinishTime(0);
			mn.setSortKey(0);
			mn.setTasks(new ArrayList<Task>());
			for (Slot slot : mn.getSlots()) {
				slot.setStartTime(0);
				slot.setCurFinishTime(0);
			}
		}
		
		for(DataNode rn : s.getCluster().getReduceNodes())
		{
			rn.setCurFinishTime(0);
			rn.setSortKey(0);
			rn.setTasks(new ArrayList<Task>());
			for (Slot slot : rn.getSlots()) {
				slot.setStartTime(0);
				slot.setCurFinishTime(0);
			}
		}
		
	}

}
