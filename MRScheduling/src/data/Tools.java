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
	public static void clearJobList(List<Job> joblist){
		for (Job job : joblist) {
			for (Task task : job.getMaps()) {
				task.setFinishTime(0);
				task.setOutputSize(0);
				task.setProcessed(false);
				task.setReduceDataNode(-1);
				task.setSortKey(0);
				task.setStartTime(0);
			}
		}
	}
	public static void clearDataNode(Cluster cluster){
		for (DataNode dn : cluster.getReduceNodes()) {
			for (Slot it_slot : dn.getSlots()) {
				it_slot.setCurFinishTime(0);
				it_slot.setStartTime(0);
			}
		}
	}
	public static void clearSchedule(Schedule s) {
		//清理作业的相关信息
		for(Job job : s.getJobs())
		{
			//MAP阶段完成时间
			job.setMapFinishTime(0);
			//REDUCE阶段完成时间
			job.setReduceFinishTime(0);
			//作业开始时间
			job.setStartTime(0);
			//作业完成时间
			job.setFinishTime(0);
			//排序键
			job.setSortKey(0);
			//惩罚代价
			job.setPenaltyCost(0);
			
			//清理MAP任务的相关信息
			for(Task map : job.getMaps())
			{
				//开始时间
				map.setStartTime(0);
				//结束时间
				map.setFinishTime(0);
				//排序键
				map.setSortKey(0);
				//是否处理过
				map.setProcessed(false);
			}
			//清理REDUCE任务的相关信息i
			for(Task reduce : job.getReduces())
			{
				//开始时间
				reduce.setStartTime(0);
				//结束时间
				reduce.setFinishTime(0);
				//排序键
				reduce.setSortKey(0);
				//是否处理过
				reduce.setProcessed(false);
			}
		}
		
		//清理集群的MAP数据节点相关信息
		for(DataNode mn : s.getCluster().getMapNodes())
		{
			//开始时间
			mn.setCurFinishTime(0);
			//排序键
			mn.setSortKey(0);
			//清空任务列表
			mn.setTasks(new ArrayList<Task>());
			//节点上的SLOT信息
			for (Slot slot : mn.getSlots()) {
				//SLOT开始时间
				slot.setStartTime(0);
				//SLOT当前任务的完成时间
				slot.setCurFinishTime(0);
			}
		}
		
		//清理集群REDUCE数据节点相关信息
		for(DataNode rn : s.getCluster().getReduceNodes())
		{
			//开始时间
			rn.setCurFinishTime(0);
			//排序键
			rn.setSortKey(0);
			//清空任务列表
			rn.setTasks(new ArrayList<Task>());
			//节点上的SLOT信息
			for (Slot slot : rn.getSlots()) {
				//SLOT开始时间
				slot.setStartTime(0);
				//SLOT当前任务的完成时间
				slot.setCurFinishTime(0);
			}
		}
		
	}

}
