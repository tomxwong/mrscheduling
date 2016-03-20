package algorithms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import data.Parameters;
import data.RandomInstance;
import data.RandomInstanceFile;
import data.Tools;

import model.Cluster;
import model.DESCJobComparator;
import model.DataNode;
import model.Job;
import model.JobComparator;
import model.Schedule;
import model.Task;
import model.Task.TaskType;

/*
 * Default method assign the task to the earliest available slot 
 */
public class DefaultMethod implements Method{

	private Schedule schedule;
	private Comparator slotCompare;
	private String rule;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//test code
		Parameters p = new Parameters();
		p.setP_io_rate(0.1);
		p.setP_jobs(100);
		p.setP_node(20);
		p.setP_rack(3);
		p.setP_map_slot(4);
		p.setP_reduce_slot(2);
		
		p.setP_map_num_miu(154);
		p.setP_map_num_sig(558);
		p.setP_map_dura_miu(50);
		p.setP_map_dura_sig(200);
		
		p.setP_reduce_num_miu(19);
		p.setP_reduce_num_sig(145);
		p.setP_reduce_dura_miu(100);
		p.setP_reduce_dura_sig(300);
		
		//extra add
		p.setP_job_deadline_miu(10000);
		p.setP_job_deadline_sigma(100);
		
		//RandomInstance ri = new RandomInstance(p, "1");
		RandomInstanceFile rf = new RandomInstanceFile(p, "J:\\Myspace\\论文\\实验工具\\TestData\\");
		try {
			Schedule s = rf.newInstance();	
			//System.out.println(s.getLowerBound_1());
			//System.out.println(s.getLowerBound_2());
			System.out.println(s.getLowerBound());
			DefaultMethod dm = new DefaultMethod(s, "");
			dm.init();
			dm.start();
			dm.output();
			
			
//			Schedule ss = rf.newInstance();//
//			System.out.println(ss.getLowerBound_1());
//			System.out.println(ss.getLowerBound_2());
//			System.out.println(ss.getLowerBound());
//			DefaultMethod dm1 = new DefaultMethod(ss, "JR");
//			dm1.init();
//			dm1.start();
//			dm1.output();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private class SlotComparator implements Comparator<Object>{
		public int compare(Object o1, Object o2)
	    {
	         DataNode n1 = (DataNode)o1;
	         DataNode n2 = (DataNode)o2;
	         if(n1.getCurFinishTime() < n2.getCurFinishTime()) 
	        	 return -1;
	         else if(n1.getCurFinishTime() == n2.getCurFinishTime()) 
	        	 return 0;
	         return 1;
	    }
	}
	public DefaultMethod(Schedule s, String rule)
	{
		this.rule = rule;
		this.schedule = s;
		this.slotCompare = new SlotComparator();
	}
	@Override
	public void init() {
		Tools.clearSchedule(schedule);
		// TODO Auto-generated method stub
		if(rule.equalsIgnoreCase("JR-LPT")){
			//DispatchingRules.JR_Dispatch(schedule);
			DispatchingRules.JR_Dispatch_InDepth(schedule);
			DispatchingRules.LPT_TaskDispatch(schedule);
		}
		if(rule.equalsIgnoreCase("JR-NEW"))
		{
			DispatchingRules.JR_Dispatch_New(schedule);
			//DispatchingRules.JR_Dispatch_InDepth(schedule);
			DispatchingRules.LPT_TaskDispatch(schedule);
		}
		schedule.setCurAvailList((ArrayList<DataNode>)schedule.getCluster().getMapNodes());
		Collections.sort(schedule.getCurAvailList(), slotCompare);
		
		//init data structures
		//List<Job> jobs = schedule.getJobs();
		//for(Job job : jobs)
			//schedule.getJobMaxMapTime().put(job.getJobID(), Long.MIN_VALUE);
		schedule.setFirstAvail(0);
		schedule.setFirstSlot(schedule.getCluster().getMapNodes().get(0));
	}

	@Override
	public void start() {
		//assign the task to the next available slot in an FIFO manner
		
		//Map Phase
		Job job = null;
		Task task = null;
		
		for(int i = 0; i < schedule.getJobs().size(); i++)
		{
			job = schedule.getJobs().get(i);
			job.setSortKey(job.getMapFinishTime());
			
			for(int j = 0; j < job.getMaps().size(); j++)
			{
				task = job.getMaps().get(j);
				
				//update mapAssignment
				//schedule.setFirstSlot((DataNode)schedule.getEarlestSlot().get());
				/*if(!schedule.getMapAssignment().containsKey(schedule.getFirstSlot().getNodeID()))
					schedule.getMapAssignment().put(schedule.getFirstSlot().getNodeID(), new ArrayList<Task>());
				schedule.getMapAssignment().get(schedule.getFirstSlot().getNodeID()).add(task);*/
				
				//must before setup time calculation
				schedule.getMapTask2Slot().put(task.getTaskID(), schedule.getFirstSlot().getNodeID());
				assign(job, task);	
			}
			
		}
		
		//reduce phase
		//reuse first avail, first slot
		Collections.sort(schedule.getJobs(), new JobComparator());
		schedule.setCurAvailList((ArrayList<DataNode>)schedule.getCluster().getReduceNodes());
		schedule.setFirstAvail(schedule.getJobs().get(0).getMapFinishTime());//first finish job of map phase
		schedule.setFirstSlot(schedule.getCluster().getReduceNodes().get(0));
		
		for(int i = 0; i < schedule.getCurAvailList().size(); i++)
			schedule.getCurAvailList().get(i).setCurFinishTime(schedule.getJobs().get(0).getMapFinishTime());
		
		for(int i = 0; i < schedule.getJobs().size(); i++)
		{
			job = schedule.getJobs().get(i);
			//job.setSortKey(job.getMapFinishTime());
			for(int j = 0; j < job.getReduces().size(); j++)
			{
				task = job.getReduces().get(j);
				
				//update reduceAssignment
				//schedule.setFirstSlot((DataNode)schedule.getEarlestSlot().get());
				/*if(!schedule.getReduceAssignment().containsKey(schedule.getFirstSlot().getNodeID()))
					schedule.getReduceAssignment().put(schedule.getFirstSlot().getNodeID(), new ArrayList<Task>());
				schedule.getReduceAssignment().get(schedule.getFirstSlot().getNodeID()).add(task);*/
				
				//must before setup time calculation
				schedule.getReduceTask2Slot().put(task.getTaskID(), schedule.getFirstSlot().getNodeID());
				
				assign(job, task);
				//System.out.println("reduce task: " + task.getTaskID() + "assigned.");
			}
			
		}
		
		int size = schedule.getCurAvailList().size();
		schedule.setMakespan(schedule.getCurAvailList().get(size - 1).getCurFinishTime());
	}
	
	private void assign(Job job, Task task)
	{
		//set setup time here
		schedule.calculateSetupTime(task, schedule.getFirstSlot());
		
		//update earlest time
		if(task.getType() == TaskType.REDUCE)
		{
			//check the map finish time and current available time
			schedule.getFirstSlot().setCurFinishTime(schedule.getFirstSlot().getCurFinishTime() > job.getMapFinishTime() ? 
					schedule.getFirstSlot().getCurFinishTime() + task.getProcessTime() + task.getSetupTime() : 
						job.getMapFinishTime() + task.getProcessTime() + task.getSetupTime());
		}
		else
			schedule.getFirstSlot().setCurFinishTime(schedule.getFirstSlot().getCurFinishTime() + 
				task.getProcessTime() + task.getSetupTime());
		//adjust slot time order(insertion sort), first one is always the earliest
		schedule.getCurAvailList().remove(0);
		//long temp = schedule.getFirstAvail();
		long newTime = schedule.getFirstSlot().getCurFinishTime();
		int k = 0;
		for(; k < schedule.getCurAvailList().size(); k++){
			if(newTime < schedule.getCurAvailList().get(k).getCurFinishTime()){
				schedule.getCurAvailList().add(k, schedule.getFirstSlot());
				break;
			}
		}
		if(k == schedule.getCurAvailList().size())
			schedule.getCurAvailList().add(schedule.getFirstSlot());
		
		//update job finish time
		if (task.getType() == TaskType.REDUCE) {
			if (newTime > job.getFinishTime())
				job.setFinishTime(newTime);
		} else {
			if(newTime > job.getMapFinishTime())
				job.setMapFinishTime(newTime);
		}
		//update earlest slot
		schedule.setFirstAvail(schedule.getCurAvailList().get(0).getCurFinishTime());
		schedule.setFirstSlot(schedule.getCurAvailList().get(0));
	}

	@Override
	public void output() {
		// TODO Auto-generated method stub
		System.out.println("The makespan of the schedule under default method is: " + schedule.getMakespan());
		//System.out.println("The relative quality of the solution is: " + 1.0 * (schedule.getMakespan() - schedule.lowerBound()) / schedule.lowerBound());
	}

	public Schedule getSchedule() {
		return schedule;
	}

	public void setSchedule(Schedule schedule) {
		this.schedule = schedule;
	}

	public Comparator getSlotCompare() {
		return slotCompare;
	}

	public void setSlotCompare(Comparator slotCompare) {
		this.slotCompare = slotCompare;
	}

}
