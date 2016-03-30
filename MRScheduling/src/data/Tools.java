package data;

import java.util.List;
import java.util.ArrayList;

import model.Cluster;
import model.DataNode;
import model.Job;
import model.Schedule;
import model.Slot;
import model.Task;

public class Tools {

	public static void clearResources(Schedule s){
		for (DataNode dn : s.getCluster().getMapNodes()) {
			for (Slot it_slot : dn.getMapSlots()) {
				it_slot.setCurFinishTime(0);
				it_slot.setStartTime(0);
			}
		}
		for (DataNode dn : s.getCluster().getReduceNodes()) {
			for (Slot it_slot : dn.getReduceSlots()) {
				it_slot.setCurFinishTime(0);
				it_slot.setStartTime(0);
			}
		}
	}
	public static void clearJobInfo(List<Job> joblist){
		for(Job job : joblist)
		{
			//MAP阶段完成时间
			job.setMapFinishTime(0);
			//REDUCE阶段完成时间
			job.setReduceFinishTime(0);
			//作业开始时间
			job.setStartTime(0);
			//作业完成时间
			job.setFinishTime(0);
			//惩罚代价
			job.setPenaltyCost(0);
			//清除排序关键字
			job.setSortKey(0);
			//清理任务信息
			for (Task task : job.getMaps()) {
				task.setFinishTime(0);
				task.setOutputSize(0);
				task.setProcessed(false);
				task.setReduceDataNode(0);
				task.setSortKey(0);
				task.setStartTime(0);
			}
			for (Task task : job.getReduces()) {
				task.setFinishTime(0);
				task.setOutputSize(0);
				task.setProcessed(false);
				task.setSortKey(0);
				task.setStartTime(0);
			}
		}
	}
}
