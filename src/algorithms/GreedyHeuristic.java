package algorithms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import data.Parameters;
import data.RandomInstance;
import data.RandomInstanceFile;


import model.Cluster;
import model.DESCJobComparator;
import model.DataNode;
import model.Job;
import model.JobComparator;
import model.Schedule;
import model.SlotComparator;
import model.Task;

public class GreedyHeuristic implements Method {

	private Schedule schedule;
	private Comparator slotCompare;
	private String rule;
	
	
	private class DESCTaskComparator implements Comparator<Object>{
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
	
	public GreedyHeuristic(Schedule s, String rule){
		this.schedule = s;
		this.rule = rule;
		this.slotCompare = new SlotComparator();
	}
	@Override
	public void init() {
		//schedule.getLowerBound();
		
		if(rule.equalsIgnoreCase("JR"))
			DispatchingRules.JR_Dispatch(schedule);
		if(rule.equalsIgnoreCase("JR-LPT"))
		{
			DispatchingRules.JR_Dispatch(schedule);
			DispatchingRules.LPT_TaskDispatch(schedule);
		}
		if(rule.equalsIgnoreCase("JR-NEW"))
		{
			DispatchingRules.JR_Dispatch_New(schedule);
			DispatchingRules.LPT_TaskDispatch(schedule);
		}
		schedule.setCurAvailList((ArrayList<DataNode>)schedule.getCluster().getMapNodes());

	}

	@Override
	public void start() {
		// assign in a greedy way
		//Map Phase
		Job job = null;
		Task task = null;
		
		for(int i = 0; i < schedule.getJobs().size(); i++)
		{
			job = schedule.getJobs().get(i);
			
			for(int j = 0; j < job.getMaps().size(); j++)
			{
				task = job.getMaps().get(j);
				long earliest = Long.MAX_VALUE;
				int node = 0;
				for(int k = 0; k < schedule.getCluster().getMapSlot() * schedule.getCluster().getNodeNum(); k++)
				{
					schedule.calculateSetupTime(task, schedule.getCurAvailList().get(k));
					if(schedule.getCurAvailList().get(k).getCurFinishTime() + task.getProcessTime() + task.getSetupTime() < earliest)
					{
						earliest = schedule.getCurAvailList().get(k).getCurFinishTime() + task.getProcessTime() + task.getSetupTime();
						node = k;
					}
				}
				//assign to the earliest possible map slot
				schedule.getMapTask2Slot().put(task.getTaskID(), schedule.getCurAvailList().get(node).getNodeID());
				task.setStartTime(schedule.getCurAvailList().get(node).getCurFinishTime());
				schedule.getCurAvailList().get(node).setCurFinishTime(earliest) ;
				task.setFinishTime(earliest);
				schedule.getCurAvailList().get(node).getTasks().add(task);
				schedule.calculateSetupTime(task, schedule.getCurAvailList().get(node));
				if(earliest > task.getJob().getMapFinishTime())
					task.getJob().setMapFinishTime(earliest);
			}
		}
		//improve();
		//reduce phase in the same way
		ArrayList<DataNode> reduces = (ArrayList<DataNode>)schedule.getCluster().getReduceNodes();
		schedule.setCurAvailList(reduces);
		for(int i = 0; i < schedule.getJobs().size(); i++)
		{
			job = schedule.getJobs().get(i);
			job.setSortKey(job.getMapFinishTime());
			Collections.sort(job.getReduces(), new DESCTaskComparator());//sort reduce task in desc order of time
		}
		Collections.sort(schedule.getJobs(), new JobComparator());
		//DispatchingRules.JR_Dispatch(schedule);
		/*for(int k = 0; k < reduces.size(); k++)
		{
			reduces.get(k).setLongest(schedule.getJobs().get(0).getReduces().get(0));
			reduces.get(k).setShortest(schedule.getJobs().get(0).getReduces().get(0));
			
		}*/
		long cmax = Long.MIN_VALUE;
		for(int i = 0; i < schedule.getJobs().size(); i++)
		{
			job = schedule.getJobs().get(i);
			
			for(int j = 0; j < job.getReduces().size(); j++)
			{
				task = job.getReduces().get(j);
				long earliest = Long.MAX_VALUE;
				int node = 0;
				long current = 0;
				for(int k = 0; k < reduces.size(); k++)
				{
					schedule.calculateSetupTime(task, reduces.get(k));
					current = Math.max(schedule.getCurAvailList().get(k).getCurFinishTime(), job.getMapFinishTime());
					if(current + task.getProcessTime() + task.getSetupTime() < earliest)
					{
						earliest = current + task.getProcessTime() + task.getSetupTime();
						node = k;
					}
				}
				//assign to the earliest possible map slot
				schedule.getReduceTask2Slot().put(task.getTaskID(), schedule.getCurAvailList().get(node).getNodeID());
				task.setStartTime(schedule.getCurAvailList().get(node).getCurFinishTime());
				schedule.getCurAvailList().get(node).setCurFinishTime(earliest) ;
				schedule.getCurAvailList().get(node).getTasks().add(task);
				
				if(earliest > task.getJob().getFinishTime())
					task.getJob().setFinishTime(earliest);
				
				/*if(task.getProcessTime() + task.getSetupTime() > reduces.get(node).getLongest().getProcessTime() + 
						reduces.get(node).getLongest().getSetupTime())
					reduces.get(node).setLongest(task);
				if(task.getProcessTime() + task.getSetupTime() < reduces.get(node).getShortest().getProcessTime() + 
						reduces.get(node).getShortest().getSetupTime())
					reduces.get(node).setShortest(task);*/
				
			}
			if(job.getFinishTime() > cmax)
				cmax = job.getFinishTime();
		}
		schedule.setMakespan(cmax);
		//System.out.println("before adjust: " + cmax);
		
		//possible adjust --------------------trying to adjust reduce phase is USUALLY in vain!--------------------
		/*int maxIter = 1;
		int count = 0;
		while(count < maxIter)
		{
			Collections.sort(reduces, new SlotComparator());
			DataNode n1, n2 = null;
			long new1, new2 = 0;
			int size1, size2 = 0;
			Task t1,t2 = null;
			long m = schedule.getMakespan();
			
			//for(int i = 0; i < reduces.size() - 1; i++)
			//{
				n1 = reduces.get(reduces.size() - 1);
				n2 = reduces.get(0);
				size1 = n1.getTasks().size();
				size2 = n2.getTasks().size();
				t1 = n1.getTasks().get(size1 - 1);//last task
				t2 = n2.getTasks().get(size2 - 1);

				int index = 0;
				for(int i = size1 / 2; i < size1; i++)
				{
					t1 = n1.getTasks().get(i);
					long diff = t1.getStartTime() - t1.getProcessTime() - t1.getSetupTime();
					if(diff > t1.getJob().getMapFinishTime())
					{
						index = i;
						break;
					}
				}
				new1 = n1.getCurFinishTime() - t1.getProcessTime()
					- t1.getSetupTime();
		

				new2 = n2.getCurFinishTime() - t2.getProcessTime()
					- t2.getSetupTime();
				for(int i = index + 1; i < size1; i++)
				{
					Task t = n1.getTasks().get(i);
					long diff = t.getStartTime() - t.getProcessTime() - t.getSetupTime();
					if(diff < t.getJob().getMapFinishTime())
						new1 += t.getJob().getMapFinishTime() - diff;
				}
				
				schedule.calculateSetupTime(t1, n2);
				schedule.calculateSetupTime(t2, n1);
				new1 = Math.max(new1, t2.getJob().getMapFinishTime())
						+ t2.getProcessTime() + t2.getSetupTime();
				new2 = Math.max(new2, t1.getJob().getMapFinishTime())
						+ t1.getProcessTime() + t1.getSetupTime();

				if (Math.max(new1, new2) < schedule.getMakespan()) {
					//m = Math.max(new1, new2);
					//schedule.setMakespan(Math.max(new1, new2));
					n1.setCurFinishTime(new1);
					n2.setCurFinishTime(new2);
					
					t1 = n1.getTasks().remove(index);
					t2 = n2.getTasks().remove(size2 - 1);
					
					n1.getTasks().add(t2);
					n2.getTasks().add(t1);
					
					Collections.sort(reduces, new SlotComparator());
					schedule.setMakespan(reduces.get(reduces.size() - 1).getCurFinishTime());
					count = 0;
				}
				else count++;
			}*/
			//else count++;
			/*Collections.sort(reduces, new SlotComparator());
			DataNode ln = reduces.get(reduces.size() - 1);
			DataNode sn = reduces.get(0);
			
			if(ln.getCurFinishTime() < schedule.getMakespan())
				schedule.setMakespan(ln.getCurFinishTime());
			else count++;
			
			long gain = ln.getLongest().getProcessTime() - sn.getShortest().getProcessTime() + 
					ln.getLongest().getSetupTime() - sn.getShortest().getSetupTime();
			
			//prevent reduce from early start
			if(ln.getCurFinishTime() - ln.getLongest().getProcessTime() - ln.getLongest().getSetupTime()
					< sn.getShortest().getJob().getMapFinishTime())
				ln.setCurFinishTime(sn.getShortest().getJob().getMapFinishTime() + sn.getShortest().getProcessTime()
						+ sn.getShortest().getSetupTime());
			else ln.setCurFinishTime(ln.getCurFinishTime() - gain);
			
			if(sn.getCurFinishTime() - sn.getShortest().getProcessTime() - sn.getShortest().getSetupTime()
					< ln.getLongest().getJob().getMapFinishTime())
				sn.setCurFinishTime(ln.getLongest().getJob().getMapFinishTime() + ln.getLongest().getProcessTime()
						+ ln.getLongest().getSetupTime());
			else sn.setCurFinishTime(sn.getCurFinishTime() + gain);*/
			
			
		//}

	}

	@Override
	public void output() {
		System.out.println("The makespan of the schedule under greedy heuristic method is: " + schedule.getMakespan());
		System.out.println("The relative quality of the solution is: " +
				1.0 * (schedule.getMakespan() - schedule.lowerBound()) / schedule.lowerBound());

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Parameters p = new Parameters();
		//p.setP_io_rate(0.1);
		p.setP_jobs(100);
		p.setP_node(10);
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
		
		p.setP_scale_bi_major_high(2);
		p.setP_scale_bi_major_low(1);
		
		p.setP_scale_bi_minor_high(10);
		p.setP_scale_bi_minor_low(8);
		
		RandomInstance ri = new RandomInstance(p, "1");
		String instance = "G:\\Research\\Data\\MR_I_BI_100_2.txt";
		RandomInstanceFile rf = new RandomInstanceFile(p, instance);
		RandomInstanceFile rf1 = new RandomInstanceFile(p, instance);
		RandomInstanceFile rf2 = new RandomInstanceFile(p, instance);

		try {
			Schedule ss = rf.newInstance();
			//System.out.println(ss.getLowerBound_1());
			//System.out.println(ss.getLowerBound_2());
			System.out.println(ss.getLowerBound());
			DefaultMethod dm1 = new DefaultMethod(ss, "JR-LPT");
			dm1.init();
			dm1.start();
			dm1.output();
			
			/*Schedule s = rf.newInstance();
			System.out.println(s.getLowerBound_1());
			System.out.println(s.getLowerBound_2());
			System.out.println(s.getLowerBound());
			dm1 = new DefaultMethod(s, "");
			dm1.init();
			dm1.start();
			dm1.output();*/
			//check(s,ss);
			/*Schedule s = rf.newInstance();
			//System.out.println(s.getLowerBound_1());
			//System.out.println(s.getLowerBound_2());
			System.out.println(s.getLowerBound());
			BackwardHeuristic bh = new BackwardHeuristic(s, "JR");
			bh.init();
			bh.start();
			bh.output();*/
			
			/*Schedule ss = rf.newInstance();
			//System.out.println(s.getLowerBound_1());
			//System.out.println(s.getLowerBound_2());
			System.out.println(ss.getLowerBound());
			BackwardHeuristic bh1 = new BackwardHeuristic(ss, "");
			bh1.init();
			bh1.start();
			bh1.output();*/
			
			Schedule s1 = rf1.newInstance();
			//System.out.println(s1.getLowerBound_1());
			//System.out.println(s1.getLowerBound_2());
			System.out.println(s1.getLowerBound());
			GreedyHeuristic gh = new GreedyHeuristic(s1, "JR-NEW");
			gh.init();
			gh.start();
			gh.output();
			
			Schedule s2 = rf2.newInstance();
			//System.out.println(s1.getLowerBound_1());
			//System.out.println(s1.getLowerBound_2());
			System.out.println(s2.getLowerBound());
			SlotGreedyHeuristic gh1 = new SlotGreedyHeuristic(s2, "JR-LPT");
			gh1.init();
			gh1.start();
			gh1.output();
			
			
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	private static void check(Schedule s, Schedule ss) {
		if(s.getJobs().size() != ss.getJobs().size())
			System.out.println(1);
		for(int i = 0; i < s.getJobs().size(); i++)
		{
			Job job1 = s.getJobs().get(i);
			Job job2 = ss.getJobs().get(i);
			
			if(job1.getMapNum() != job2.getMapNum())
				System.out.println(4);
			if(job1.getReduceNum() != job2.getReduceNum())
				System.out.println(5);
			for(int j = 0; j < job1.getMapNum(); j++)
			{
				Task t1 = job1.getMaps().get(j);
				Task t2 = job2.getMaps().get(j);
				
				if(t1.getProcessTime() != t2.getProcessTime())
					System.out.println(2);
				if(t1.getInputSize() != t2.getInputSize())
					System.out.println(3);
			}
		}
		System.out.println(0);
	}

}
