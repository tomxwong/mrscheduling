package algorithms;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import model.Cluster;
import model.DESCJobComparator;
import model.DESCTaskComparator;
import model.Job;
import model.JobComparator;
import model.Schedule;
import model.Task;

public class DispatchingRules {

	public static void LPT_MapTask(Schedule s)
	{
		Collections.sort(s.getMaps(), new DESCTaskComparator());
	}
	public static void LPT_TaskDispatch(Schedule s)
	{
		Job job = null;
		for(int i = 0; i < s.getJobs().size(); i++)
		{
			job = s.getJobs().get(i);
			Collections.sort(job.getMaps(), new DESCTaskComparator());
			Collections.sort(job.getReduces(), new DESCTaskComparator());
		}
	}
	public static void LPT_Job_Dispatch(Schedule s)
	{
		long sumMap = 0;
		long sumReduce = 0;
		List<Job> first = new ArrayList<Job>();
		List<Job> second = new ArrayList<Job>();
		for(int i = 0; i < s.getJobs().size(); i++)
		{
			Job job = s.getJobs().get(i);
			sumMap = 0;
			sumReduce = 0;
			
			//sort the tasks in each map in LPT rule
			//Collections.sort(job.getMaps(), new DESCTaskComparator());
			//Collections.sort(job.getReduces(), new DESCTaskComparator());
			
			for(int j = 0; j < job.getMaps().size(); j++)
			{
				Task task = job.getMaps().get(j);
				sumMap += task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE;
			}
			for(int j = 0; j < job.getReduces().size(); j++)
			{
				Task task = job.getReduces().get(j);
				sumReduce += task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE;
			}
			double a = job.getMapNum() <= s.getCluster().getNodeNum() * s.getCluster().getMapSlot() ? 
					sumMap / job.getMapNum() : sumMap / (s.getCluster().getNodeNum() * s.getCluster().getMapSlot());
					
			double b = job.getReduceNum() <= s.getCluster().getNodeNum() * s.getCluster().getReduceSlot() ? 
					sumReduce / job.getReduceNum() : sumReduce / (s.getCluster().getNodeNum() * s.getCluster().getReduceSlot());
					
			/*if(a < b){
				job.setSortKey(a);
				//job.setJRValue(a);
				first.add(job);		
			}
			else{
				job.setSortKey(b);
				//job.setJRValue(b);
				second.add(job);
			}*/
			job.setSortKey(b);
				
		}
		/*Collections.sort(first, new JobComparator());
		Collections.sort(second, new DESCJobComparator());
		
		first.addAll(second);*/
		Collections.sort(s.getJobs(), new DESCJobComparator());
		//s.setJobs(first);
	}
	public static void JR_Dispatch_New(Schedule s)
	{
		int mslots = s.getCluster().getNodeNum() * s.getCluster().getMapSlot();
		int rslots = s.getCluster().getNodeNum() * s.getCluster().getReduceSlot();
		double omega = s.getCluster().getOmega();
		
		long sumMap = 0;
		long sumReduce = 0;
		List<Job> first = new ArrayList<Job>();
		List<Job> second = new ArrayList<Job>();
		for(int i = 0; i < s.getJobs().size(); i++)
		{
			Job job = s.getJobs().get(i);
			sumMap = 0;
			sumReduce = 0;
			
			//sort the tasks in each map in LPT rule
			//Collections.sort(job.getMaps(), new DESCTaskComparator());
			//Collections.sort(job.getReduces(), new DESCTaskComparator());
			double maxMap = Double.MIN_VALUE;
			double maxReduce = Double.MIN_VALUE;
			
			for(int j = 0; j < job.getMaps().size(); j++)
			{
				Task task = job.getMaps().get(j);
				if(task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE > maxMap)
					maxMap = task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE;
				sumMap += task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE;
			}
			for(int j = 0; j < job.getReduces().size(); j++)
			{
				Task task = job.getReduces().get(j);
				if(task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE > maxReduce)
					maxReduce = task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE;
				sumReduce += task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE;
			}
			//double a = ((sumMap / mslots) + (sumMap / job.getMapNum() * (job.getMapNum() - 1) / mslots + maxMap)) / 2;
			//double b = ((sumReduce / rslots) + (sumReduce / job.getReduceNum() * (job.getReduceNum() - 1) / rslots + maxReduce)) / 2;
			double a = (sumMap / mslots) * omega + (sumMap / job.getMapNum() * (job.getMapNum() - 1) / mslots + maxMap) * (1 - omega);
			double b = (sumReduce / rslots) * omega + (sumReduce / job.getReduceNum() * (job.getReduceNum() - 1) / rslots + maxReduce) * (1 - omega);

			//double a = job.getMapNum() <= s.getCluster().getNodeNum() * s.getCluster().getMapSlot() ? 
					//sumMap / job.getMapNum() : sumMap / (s.getCluster().getNodeNum() * s.getCluster().getMapSlot());
					
			//double b = job.getReduceNum() <= s.getCluster().getNodeNum() * s.getCluster().getReduceSlot() ? 
					//sumReduce / job.getReduceNum() : sumReduce / (s.getCluster().getNodeNum() * s.getCluster().getReduceSlot());
			
			
			if(a < b){
				job.setSortKey(a);
				//job.setJRValue(a);
				first.add(job);		
			}
			else{
				job.setSortKey(b);
				//job.setJRValue(b);
				second.add(job);
			}
				
		}
		Collections.sort(first, new JobComparator());
		Collections.sort(second, new DESCJobComparator());
		
		first.addAll(second);
		s.setJobs(first);
	}
	public static void JR_Dispatch(Schedule s) {
		
		long sumMap = 0;
		long sumReduce = 0;
		List<Job> first = new ArrayList<Job>();
		List<Job> second = new ArrayList<Job>();
		for(int i = 0; i < s.getJobs().size(); i++)
		{
			Job job = s.getJobs().get(i);
			sumMap = 0;
			sumReduce = 0;
			
			//sort the tasks in each map in LPT rule
			//Collections.sort(job.getMaps(), new DESCTaskComparator());
			//Collections.sort(job.getReduces(), new DESCTaskComparator());
			
			for(int j = 0; j < job.getMaps().size(); j++)
			{
				Task task = job.getMaps().get(j);
				sumMap += task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE;
			}
			for(int j = 0; j < job.getReduces().size(); j++)
			{
				Task task = job.getReduces().get(j);
				sumReduce += task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE;
			}
			double a = job.getMapNum() <= s.getCluster().getNodeNum() * s.getCluster().getMapSlot() ? 
					sumMap / job.getMapNum() : sumMap / (s.getCluster().getNodeNum() * s.getCluster().getMapSlot());
					
			double b = job.getReduceNum() <= s.getCluster().getNodeNum() * s.getCluster().getReduceSlot() ? 
					sumReduce / job.getReduceNum() : sumReduce / (s.getCluster().getNodeNum() * s.getCluster().getReduceSlot());
					
			if(a < b){
				job.setSortKey(a);
				//job.setJRValue(a);
				first.add(job);		
			}
			else{
				job.setSortKey(b);
				//job.setJRValue(b);
				second.add(job);
			}
				
		}
		Collections.sort(first, new JobComparator());
		Collections.sort(second, new DESCJobComparator());
		
		first.addAll(second);
		s.setJobs(first);
		
		/*int mslots = s.getCluster().getNodeNum() * s.getCluster().getMapSlot();
		int rslots = s.getCluster().getNodeNum() * s.getCluster().getReduceSlot();
		
		long sumMap = 0;
		long sumReduce = 0;
		List<Job> first = new ArrayList<Job>();
		List<Job> second = new ArrayList<Job>();
		for(int i = 0; i < s.getJobs().size(); i++)
		{
			Job job = s.getJobs().get(i);
			sumMap = 0;
			sumReduce = 0;
			
			//sort the tasks in each map in LPT rule
			//Collections.sort(job.getMaps(), new DESCTaskComparator());
			//Collections.sort(job.getReduces(), new DESCTaskComparator());
			double maxMap = Double.MIN_VALUE;
			double maxReduce = Double.MIN_VALUE;
			
			for(int j = 0; j < job.getMaps().size(); j++)
			{
				Task task = job.getMaps().get(j);
				if(task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE > maxMap)
					maxMap = task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE;
				sumMap += task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE;
			}
			for(int j = 0; j < job.getReduces().size(); j++)
			{
				Task task = job.getReduces().get(j);
				if(task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE > maxReduce)
					maxReduce = task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE;
				sumReduce += task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE;
			}
			int mapMin = Math.min(job.getMapNum(), mslots);
			int reduceMin = Math.min(job.getReduceNum(), rslots);
			
			double a = ((sumMap * 1.0 / mapMin) + (sumMap * 1.0 / job.getMapNum() * (job.getMapNum() - 1) / mapMin + maxMap)) / 2;
			double b = ((sumReduce * 1.0 / rslots) + (sumReduce * 1.0 / job.getReduceNum() * (job.getReduceNum() - 1) / rslots + maxReduce)) / 2;

			//double a = job.getMapNum() <= s.getCluster().getNodeNum() * s.getCluster().getMapSlot() ? 
					//sumMap / job.getMapNum() : sumMap / (s.getCluster().getNodeNum() * s.getCluster().getMapSlot());
					
			//double b = job.getReduceNum() <= s.getCluster().getNodeNum() * s.getCluster().getReduceSlot() ? 
					//sumReduce / job.getReduceNum() : sumReduce / (s.getCluster().getNodeNum() * s.getCluster().getReduceSlot());
			
			
			if(a < b){
				job.setSortKey(a);
				//job.setJRValue(a);
				first.add(job);		
			}
			else{
				job.setSortKey(b);
				//job.setJRValue(b);
				second.add(job);
			}
				
		}
		Collections.sort(first, new JobComparator());
		Collections.sort(second, new DESCJobComparator());
		
		first.addAll(second);
		s.setJobs(first);*/
	}
	public static void JR_Dispatch_InDepth(Schedule s)
	{
		int mslots = s.getCluster().getNodeNum() * s.getCluster().getMapSlot();
		int rslots = s.getCluster().getNodeNum() * s.getCluster().getReduceSlot();
		
		long sumMap = 0;
		long sumReduce = 0;
		List<Job> first = new ArrayList<Job>();
		List<Job> second = new ArrayList<Job>();
		for(int i = 0; i < s.getJobs().size(); i++)
		{
			Job job = s.getJobs().get(i);
			sumMap = 0;
			sumReduce = 0;
			
			//sort the tasks in each map in LPT rule
			//Collections.sort(job.getMaps(), new DESCTaskComparator());
			//Collections.sort(job.getReduces(), new DESCTaskComparator());
			double maxMap = Double.MIN_VALUE;
			double maxReduce = Double.MIN_VALUE;
			
			for(int j = 0; j < job.getMaps().size(); j++)
			{
				Task task = job.getMaps().get(j);
				if(task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE > maxMap)
					maxMap = task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE;
				sumMap += task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE;
			}
			for(int j = 0; j < job.getReduces().size(); j++)
			{
				Task task = job.getReduces().get(j);
				if(task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE > maxReduce)
					maxReduce = task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE;
				sumReduce += task.getProcessTime() + task.getInputSize() / Cluster.LOCAL_RATE;
			}
			//int mapMin = Math.min(job.getMapNum(), mslots);
			//int reduceMin = Math.min(job.getReduceNum(), rslots);
			
			double a,b;
			if(job.getMapNum() < mslots)
				a = ((sumMap * 1.0 / mslots) + (sumMap * 1.0 / job.getMapNum() * (job.getMapNum() - 1) / mslots + maxMap)) / 2;
			else
				a = ((sumMap * 1.0 / mslots) + (sumMap * 1.0 / job.getMapNum() * (job.getMapNum() - 2) / mslots + 2 * maxMap)) / 2;
						
			if(job.getReduceNum() < rslots)
				//b = sumReduce * 1.0 / rslots;
				b = ((sumReduce * 1.0 / rslots) + (sumReduce * 1.0 / job.getReduceNum() * (job.getReduceNum() - 1) / rslots + maxReduce)) / 2;
			else
				b = ((sumReduce * 1.0 / rslots) + (sumReduce * 1.0 / job.getReduceNum() * (job.getReduceNum() - 2) / rslots + 2 * maxReduce)) / 2;
				
		    
			if(a < b){
				job.setSortKey(a);
				//job.setJRValue(a);
				first.add(job);		
			}
			else{
				job.setSortKey(b);
				//job.setJRValue(b);
				second.add(job);
			}
				
		}
		Collections.sort(first, new JobComparator());
		Collections.sort(second, new DESCJobComparator());
		
		first.addAll(second);
		s.setJobs(first);
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	public static void LPT_ReduceTask(Schedule s) {
		Collections.sort(s.getReduces(), new DESCTaskComparator());
		
	}

}
