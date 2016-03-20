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
import model.DESCTaskComparator;
import model.DataNode;
import model.Job;
import model.JobComparator;
import model.Schedule;
import model.SlotComparator;
import model.SlotKeyComparator;
import model.Task;

public class SlotGreedyHeuristicLocal implements Method {

	private Schedule schedule;
	private Comparator slotCompare;
	private String rule;
	
	
	public SlotGreedyHeuristicLocal(Schedule s, String rule){
		this.schedule = s;
		this.rule = rule;
		this.slotCompare = new SlotComparator();
	}
	@Override
	public void init() {
		//schedule.getLowerBound();
		Tools.clearSchedule(schedule);
		
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
		// assign a map task to the slot in its hostlist
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
					int local = schedule.calculateSetupTime(task, schedule.getCurAvailList().get(k));
					if(local >= 2) continue;
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
				
				/*int key = schedule.getCurAvailList().get(node).getNodeID() % schedule.getCluster().getNodeNum();
				if(job.getSlotMapTaskNum().get(key) != null)
				{
					int oldNum = job.getSlotMapTaskNum().get(key);
					job.getSlotMapTaskNum().remove(key);
					job.getSlotMapTaskNum().put(key, ++oldNum);
				}else
					job.getSlotMapTaskNum().put(key, 0);*/
			}
		}
		//ImproveMethods.improve(schedule);
		
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
		
		/*for(int k = 0; k < reduces.size(); k++)
		{
			reduces.get(k).setLongest(schedule.getJobs().get(0).getReduces().get(0));
			reduces.get(k).setShortest(schedule.getJobs().get(0).getReduces().get(0));
			
		}*/
		double mapRate = 0.0;
		long cmax = Long.MIN_VALUE;
		ArrayList<DataNode> jobSlotList = null;
		for(int i = 0; i < schedule.getJobs().size(); i++)
		{
			job = schedule.getJobs().get(i);
			jobSlotList = new ArrayList<DataNode>();
			int maxNum = Integer.MIN_VALUE;
			//int key = job.getSlotMapTaskNum().keySet().iterator().next();
			/*for(int s = 0; s < reduces.size(); s++){
				if(job.getSlotMapTaskNum().keySet().contains(
						reduces.get(s).getNodeID() % schedule.getCluster().getNodeNum()))
						//if(job.getSlotMapTaskNum().get(reduces.get(s).getNodeID() % schedule.getCluster().getNodeNum())
								//> job.getMapNum() * mapRate)
					reduces.get(s).setSortKey(
							job.getSlotMapTaskNum().get(reduces.get(s).getNodeID() % schedule.getCluster().getNodeNum()));
				else
					reduces.get(s).setSortKey(0);
							
			}
			Collections.sort(reduces, new SlotKeyComparator());*/
			double num = reduces.get(7).getSortKey();
			double rate = 1;
			/*for(int jay = 0; jay < reduces.size() * rate; jay++){
				//if(reduces.get(jay).getSortKey() < num) break;
				jobSlotList.add(reduces.get(jay));
			}*/
			//int lastKey = 0;
			for(int j = 0; j < job.getReduces().size(); j++)
			{
				task = job.getReduces().get(j);
				
				/*if(!jobSlotList.isEmpty())
				{
					long earliest = Long.MAX_VALUE;
					int node = 0;
					long current = 0;
					for(int k = 0; k < jobSlotList.size() ; k++)
					{
						if(jobSlotList.get(k).getNodeID() % schedule.getCluster().getNodeNum() == lastKey)
							continue;
						schedule.calculateSetupTime(task, jobSlotList.get(k));
						current = Math.max(jobSlotList.get(k).getCurFinishTime(), job.getMapFinishTime());
						if(current + task.getProcessTime() + task.getSetupTime() < earliest)
						{
							earliest = current + task.getProcessTime() + task.getSetupTime();
							node = k;
						}
					}
					//assign to the earliest possible map slot
					schedule.getReduceTask2Slot().put(task.getTaskID(), jobSlotList.get(node).getNodeID());
					jobSlotList.get(node).setCurFinishTime(earliest) ;
					jobSlotList.get(node).getTasks().add(task);
					
					if(earliest > task.getJob().getFinishTime())
						task.getJob().setFinishTime(earliest);
					
					//lastKey = jobSlotList.get(node).getNodeID() % schedule.getCluster().getNodeNum();
				}*/
				/*else
				{*/
					long earliest = Long.MAX_VALUE;
					int node = 0;
					long current = 0;
					for (int k = 0; k <= reduces.size() - 1; k++) {
						int local = schedule.calculateSetupTime(task, reduces.get(k));
						//if(local > 2)continue;
						current = Math.max(reduces.get(k)
								.getCurFinishTime(), job.getMapFinishTime());
						if (current + task.getProcessTime()
								+ task.getSetupTime() < earliest) {
							earliest = current + task.getProcessTime()
									+ task.getSetupTime();
							node = k;
						}
					}
					// assign to the earliest possible map slot
					schedule.getReduceTask2Slot().put(task.getTaskID(),
							reduces.get(node).getNodeID());
					reduces.get(node)
							.setCurFinishTime(earliest);
					reduces.get(node).getTasks().add(task);

					if (earliest > task.getJob().getFinishTime())
						task.getJob().setFinishTime(earliest);
					
					//lastKey = schedule.getCurAvailList().get(node).getNodeID() % schedule.getCluster().getNodeNum();
				//}
				
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
		

	}

	private DataNode bestSlot(Task task) {
		long earliest = Long.MAX_VALUE;
		DataNode node = null;
		for(DataNode mapSlot : schedule.getCurAvailList())
		{
			schedule.calculateSetupTime(task, mapSlot);
			if(mapSlot.getCurFinishTime() + task.getProcessTime() + task.getSetupTime() < earliest)
			{
				earliest = mapSlot.getCurFinishTime() + task.getProcessTime() + task.getSetupTime();
				node = mapSlot;
			}
		}
		return node;
	}
	private DataNode rackSlot(DataNode lastSlot, Task task) {
//ArrayList<Integer> list = task.getHostList();
		
		//DataNode first = null;
		DataNode best = null;
		long min = Long.MAX_VALUE;
		
		for(DataNode mapSlot : schedule.getCurAvailList()){
			if(schedule.getCluster().getTopo()[lastSlot.getNodeID()][mapSlot.getNodeID()] == 2)
			{
				//if(first == null) first = mapSlot;
				//if(list.contains(mapSlot.getNodeID()))
					//return mapSlot;
				schedule.calculateSetupTime(task, mapSlot);
				if(mapSlot.getCurFinishTime() + task.getSetupTime() + task.getProcessTime() < min)
				{
					min = mapSlot.getCurFinishTime() + task.getSetupTime() + task.getProcessTime();
					best = mapSlot;
				}
			}
		}
		return best;
	}
	private DataNode localSlot(DataNode lastSlot, Task task) {
		//ArrayList<Integer> list = task.getHostList();
		
		//DataNode first = null;
		DataNode best = null;
		long min = Long.MAX_VALUE;
		
		for(DataNode mapSlot : schedule.getCurAvailList()){
			if(schedule.getCluster().getTopo()[lastSlot.getNodeID()][mapSlot.getNodeID()] == 1)
			{
				//if(first == null) first = mapSlot;
				//if(list.contains(mapSlot.getNodeID()))
					//return mapSlot;
				schedule.calculateSetupTime(task, mapSlot);
				if(mapSlot.getCurFinishTime() + task.getSetupTime() + task.getProcessTime() < min)
				{
					min = mapSlot.getCurFinishTime() + task.getSetupTime() + task.getProcessTime();
					best = mapSlot;
				}
			}
		}
		return best;
	}
	@Override
	public void output() {
		System.out.println("The makespan of the schedule under slot greedy heuristic method is: " + schedule.getMakespan());
		//System.out.println("The relative quality of the solution is: " +
				//1.0 * (schedule.getMakespan() - schedule.lowerBound()) / schedule.lowerBound());

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
		RandomInstanceFile rf = new RandomInstanceFile(p, "G:\\Research\\Data\\MR_I_Jay_1.txt");
		RandomInstanceFile rf1 = new RandomInstanceFile(p, "G:\\Research\\Data\\MR_I_Jay_1.txt");
		try {
			Schedule ss = ri.newInstance();
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
			
			Schedule s1 = rf.newInstance();
			//System.out.println(s1.getLowerBound_1());
			//System.out.println(s1.getLowerBound_2());
			System.out.println(s1.getLowerBound());
			GreedyHeuristic gh = new GreedyHeuristic(s1, "JR");
			gh.init();
			gh.start();
			gh.output();
			
			Schedule s2 = rf1.newInstance();
			//System.out.println(s1.getLowerBound_1());
			//System.out.println(s1.getLowerBound_2());
			System.out.println(s2.getLowerBound());
			SlotGreedyHeuristicLocal gh1 = new SlotGreedyHeuristicLocal(s2, "JR");
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
