package algorithms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import data.Parameters;
import data.RandomInstance;
import data.RandomInstanceFile;
import data.Tools;


import model.DESCTaskComparator;
import model.DataNode;
import model.Job;
import model.JobComparator;
import model.Schedule;
import model.SlotComparator;
import model.Task;
import model.TaskComparator;

public class LPTTaskGreedy implements Method {

	private Schedule schedule;
	private Comparator slotCompare;
	private String rule;
	
	public LPTTaskGreedy(Schedule s, String rule){
		this.schedule = s;
		this.rule = rule;
		this.slotCompare = new SlotComparator();
	}
	@Override
	public void init() {
		Tools.clearSchedule(schedule);
		//schedule.getLowerBound();
		
		if(rule.equalsIgnoreCase("JR"))
			DispatchingRules.JR_Dispatch(schedule);
		if(rule.equalsIgnoreCase("JR-LPT"))
		{
			//DispatchingRules.JR_Dispatch(schedule);
			DispatchingRules.JR_Dispatch_InDepth(schedule);
			DispatchingRules.LPT_TaskDispatch(schedule);
		}
		if(rule.equalsIgnoreCase("JR-NEW"))
		{
			DispatchingRules.JR_Dispatch_New(schedule);
			DispatchingRules.LPT_MapTask(schedule);//DispatchingRules.LPT_TaskDispatch(schedule);
		}
		if(rule.equalsIgnoreCase("LPT-TASK")){
			DispatchingRules.JR_Dispatch(schedule);
			DispatchingRules.LPT_MapTask(schedule);
		}
		if(rule.equalsIgnoreCase("LPT-JOB")){
			DispatchingRules.LPT_Job_Dispatch(schedule);
			//DispatchingRules.LPT_MapTask(schedule);
			//DispatchingRules.LPT_ReduceTask(schedule);
		}
		schedule.setCurAvailList((ArrayList<DataNode>)schedule.getCluster().getMapNodes());

	}

	@Override
	public void start() {
		// map
		Task task = null;
		for(int i = 0; i < schedule.getMaps().size(); i++)
		{
			task = schedule.getMaps().get(i);
			long earliest = Long.MAX_VALUE;
			int node = 0;
			for(int k = 0; k < schedule.getCluster().getMapSlot() * schedule.getCluster().getNodeNum(); k++)
			{
				int local = schedule.calculateSetupTime(task, schedule.getCurAvailList().get(k));
				//if(local >= 2) continue;
				if(schedule.getCurAvailList().get(k).getCurFinishTime() + task.getProcessTime() + task.getSetupTime() < earliest)
				{
					earliest = schedule.getCurAvailList().get(k).getCurFinishTime() + task.getProcessTime() + task.getSetupTime();
					node = k;
				}
			}
			//assign to the earliest possible map slot
			schedule.calculateSetupTime(task, schedule.getCurAvailList().get(node));
			schedule.getMapTask2Slot().put(task.getTaskID(), schedule.getCurAvailList().get(node).getNodeID());
			schedule.getCurAvailList().get(node).setCurFinishTime(earliest) ;
			schedule.getCurAvailList().get(node).getTasks().add(task);
			
			if(earliest > task.getJob().getMapFinishTime())
				task.getJob().setMapFinishTime(earliest);
		}
		//adjust the tasks according to job's JR order on each slot
		for(int i = 0; i < schedule.getJobs().size(); i++)
			schedule.getJobs().get(i).setMapFinishTime(0);
		for(int s = 0; s < schedule.getCluster().getMapNodes().size(); s++)
		{
			DataNode slot = schedule.getCluster().getMapNodes().get(s);
			for(int t = 0; t < slot.getTasks().size(); t++)
			{
				Task map = slot.getTasks().get(t);
				//if(s == 0) map.getJob().setMapFinishTime(0);
				map.setSortKey(schedule.getJobs().indexOf(map.getJob()));
			}
			Collections.sort(slot.getTasks(), new TaskComparator());
			
			//update map finish time
			//int lastID = slot.getTasks().get(0).getTaskID();
			long currentTime = 0;
			for(int t = 0; t < slot.getTasks().size(); t++)
			{
				Task map = slot.getTasks().get(t);
				map.setStartTime(currentTime);
				map.setFinishTime(currentTime + map.getProcessTime() + map.getSetupTime());
				
				currentTime = map.getFinishTime();
				
				if(currentTime > map.getJob().getMapFinishTime())
					map.getJob().setMapFinishTime(map.getFinishTime());
				
			}
			/*if(currentTime != slot.getCurFinishTime()){
				System.err.println("Method error!");
				slot.setCurFinishTime(currentTime);
			}*/
		}
		
		//reduce
		Job job = null;
		ArrayList<DataNode> reduces = (ArrayList<DataNode>)schedule.getCluster().getReduceNodes();
		schedule.setCurAvailList(reduces);
		for(int i = 0; i < schedule.getJobs().size(); i++)
		{
			job = schedule.getJobs().get(i);
			job.setSortKey(job.getMapFinishTime());
			Collections.sort(job.getReduces(), new DESCTaskComparator());//sort reduce task in desc order of time
		}
		Collections.sort(schedule.getJobs(), new JobComparator());
		
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
					current = Math.max(reduces.get(k).getCurFinishTime(), job.getMapFinishTime());
					if(current + task.getProcessTime() + task.getSetupTime() < earliest)
					{
						earliest = current + task.getProcessTime() + task.getSetupTime();
						node = k;
					}
				}
				//assign to the earliest possible map slot
				schedule.getReduceTask2Slot().put(task.getTaskID(), reduces.get(node).getNodeID());
				reduces.get(node).setCurFinishTime(earliest) ;
				reduces.get(node).getTasks().add(task);
				
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

	}

	@Override
	public void output() {
		System.out.println("The makespan of the schedule under LPT task greedy heuristic method is: " + schedule.getMakespan());
		/*System.out.println("The relative quality of the solution is: " +
				1.0 * (schedule.getMakespan() - schedule.lowerBound()) / schedule.lowerBound());*/

	}
	
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
		RandomInstanceFile rf = new RandomInstanceFile(p, "G:\\Research\\Data\\MR_I_BI_100_1.txt");
		RandomInstanceFile rf1 = new RandomInstanceFile(p, "G:\\Research\\Data\\MR_I_BI_100_1.txt");
		RandomInstanceFile rf2 = new RandomInstanceFile(p, "G:\\Research\\Data\\MR_I_BI_100_1.txt");
		try {
			Schedule ss = rf2.newInstance();
			//System.out.println(ss.getLowerBound_1());
			//System.out.println(ss.getLowerBound_2());
			System.out.println(ss.getLowerBound());
			DefaultMethod dm1 = new DefaultMethod(ss, "JR-LPT");
			dm1.init();
			dm1.start();
			dm1.output();
			
			/*Schedule s = rf2.newInstance();
			System.out.println(s.getLowerBound_1());
			System.out.println(s.getLowerBound_2());
			System.out.println(s.getLowerBound());
			GreedyHeuristic bh = new GreedyHeuristic(s, "JR-LPT");
			bh.init();
			bh.start();
			bh.output();*/
			
			Schedule s1 = rf.newInstance();
			//System.out.println(s1.getLowerBound_1());
			//System.out.println(s1.getLowerBound_2());
			System.out.println(s1.getLowerBound());
			SlotGreedyHeuristic gh = new SlotGreedyHeuristic(s1, "JR-LPT");
			gh.init();
			gh.start();
			gh.output();
			
			Schedule s2 = rf1.newInstance();
			//System.out.println(s1.getLowerBound_1());
			//System.out.println(s1.getLowerBound_2());
			System.out.println(s2.getLowerBound());
			LPTTaskGreedy gh1 = new LPTTaskGreedy(s2, "LPT-TASK");
			gh1.init();
			gh1.start();
			gh1.output();
			
			
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}

	}


}
