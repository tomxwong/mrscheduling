package algorithms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import data.Parameters;
import data.RandomInstance;
import data.RandomInstanceFile;

import model.Cluster;
import model.DESCJobComparator;
import model.DataNode;
import model.Job;
import model.JobComparator;
import model.Schedule;
import model.Task;
import model.TaskComparator;

public class BackwardHeuristic implements Method {

	private Schedule schedule;
	private Comparator slotCompare;
	private String rule;
	
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
	
	public BackwardHeuristic(Schedule s, String rule){
		this.schedule = s;
		this.rule = rule;
		this.slotCompare = new SlotComparator();
	}
	@Override
	public void init() {
		//schedule.getLowerBound();
		
		if(rule.equalsIgnoreCase("JR-LPT"))
			//DispatchingRules.JR_Dispatch(schedule);
		{
			DispatchingRules.JR_Dispatch(schedule);
			DispatchingRules.LPT_TaskDispatch(schedule);
		}else if(rule.equalsIgnoreCase("JR"))
			DispatchingRules.JR_Dispatch(schedule);
		
		schedule.setCurAvailList((ArrayList<DataNode>)schedule.getCluster().getMapNodes());
		//Collections.sort(schedule.getCurAvailList(), slotCompare);
		
		//init data structures
		//List<Job> jobs = schedule.getJobs();
		//for(Job job : jobs)
			//schedule.getJobMaxMapTime().put(job.getJobID(), Long.MIN_VALUE);
		//schedule.setFirstAvail(0);
		//schedule.setFirstSlot(schedule.getCluster().getMapNodes().get(0));

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
				//assign to the earliest map slot
				schedule.getMapTask2Slot().put(task.getTaskID(), schedule.getCurAvailList().get(node).getNodeID());
				task.setStartTime(schedule.getCurAvailList().get(node).getCurFinishTime());
				schedule.getCurAvailList().get(node).setCurFinishTime(earliest) ;
				task.setFinishTime(earliest);
				schedule.getCurAvailList().get(node).getTasks().add(task);
				schedule.calculateSetupTime(task, schedule.getCurAvailList().get(node));
				/*if(earliest > schedule.getJobMaxMapTime().get(job.getJobID()))
				{
					schedule.getJobMaxMapTime().remove(job.getJobID());
					schedule.getJobMaxMapTime().put(job.getJobID(), earliest);
				}
				*/
				task.setSortKey(task.getProcessTime() + task.getSetupTime());
				if(earliest > task.getJob().getMapFinishTime())
					task.getJob().setMapFinishTime(earliest);
			}
		}
		//improve();
		//and backward
		long cLow = schedule.lowerBound();
		long cUp = 0;
		double cMax = schedule.lowerBound();
		long curMin = Long.MAX_VALUE;
		double interval = 1;
		double scale = 0.001;

		long delta = 1;//(long)(cLow * scale);
		
		ArrayList<DataNode> reduces = (ArrayList<DataNode>)schedule.getCluster().getReduceNodes();
		schedule.setCurAvailList(reduces);
		for(int i = 0; i < schedule.getJobs().size(); i++)
		{
			job = schedule.getJobs().get(i);
			job.setSortKey(job.getMapFinishTime());
		    Collections.sort(job.getReduces(), new DESCTaskComparator());
		}
		Collections.sort(schedule.getJobs(), new DESCJobComparator());
		/*for(int i = 0; i < reduces.size(); i++)
			reduces.get(i).setCurFinishTime((long)cMax);*/
		for(int t = 1; t <= 2 * delta - 1; t++){
			cLow = schedule.lowerBound() - delta + t;
			cUp = 0;
			cMax = cLow;
			
			while (true) {
				for (int i = 0; i < reduces.size(); i++)
					reduces.get(i).setCurFinishTime((long) cMax);
				boolean feasible = true;
				boundUpdate: for (int i = 0; i < schedule.getJobs().size(); i++) {
					job = schedule.getJobs().get(i);

					for (int j = 0; j < job.getReduces().size(); j++) {
						task = job.getReduces().get(j);
						long longest = Long.MIN_VALUE;
						int node = 0;
						for (int k = 0; k < reduces.size(); k++) {
							schedule.calculateSetupTime(task, reduces.get(k));
							if (reduces.get(k)
									.getCurFinishTime()
									- task.getProcessTime()
									- task.getSetupTime() > longest) {
								longest = reduces.get(k)
										.getCurFinishTime()
										- task.getProcessTime()
										- task.getSetupTime();
								node = k;
							}
						}
						reduces.get(node)
								.setCurFinishTime(longest);
						//schedule.getReduceTask2Slot().put(task.getTaskID(), reduces.get(node).getNodeID());
						//reduces.get(node).getTasks().add(task);
						
						if (longest < task.getJob().getMapFinishTime()) {
							// infeasible assignmet
							if (cUp == 0)
								cMax *= (1 + scale);
							feasible = false;
							break boundUpdate;
						}
						
					}
					//adjust: move the first of the longest to the first of the shortest
					//improve(reduces);
				}
				// stop criterion version 1
				if (feasible)
					cUp = (long) cMax;
				else {
					if (cUp == 0)//have not found a cUp, do not change cLow
						continue;
					else cLow = (long) cMax;
				}

				if (cUp - cLow > interval)
					cMax = (cUp + cLow) / 2;
				else
					break;
				// end version 1
				// version 2
				// if(feasible)
				// continue;
				// end version 2
			}
			if(cUp < curMin)
				curMin = cUp;
		}
		//improve(cUp);
		schedule.setMakespan(curMin);

	}

	/*private void improve() {
		ArrayList<DataNode> slots = schedule.getCurAvailList();
		Collections.sort(slots, new SlotComparator());
		
		int count = 0;
		int maxIter = 10;
		while (count < maxIter) {
			DataNode shortest = slots.get(0);
			DataNode longest = slots.get(slots.size() - 1);

			//long stime = shortest.getCurFinishTime();
			//long ltime = longest.getCurFinishTime();

			int sp = shortest.getTasks().size();
			int sr = longest.getTasks().size();

			// Collections.sort(longest.getTasks(), new TaskComparator());
			// Collections.sort(shortest.getTasks(), new TaskComparator());
			int si = getShortestTask(shortest.getTasks());
			int li = getLongestTask(longest.getTasks());
			Task ts = shortest.getTasks().remove(si);
			Task tl = longest.getTasks().remove(li);
			long oldsSetup = ts.getSetupTime(); 
			long oldlSetup = tl.getSetupTime();
			
			
			schedule.calculateSetupTime(ts, longest);
			long oldStart = ts.getStartTime();
			ts.setStartTime(tl.getStartTime());
			ts.setFinishTime(ts.getStartTime() + ts.getProcessTime() + ts.getSetupTime());
			schedule.calculateSetupTime(tl, shortest);
			tl.setStartTime(oldStart);
			tl.setFinishTime(tl.getStartTime() + tl.getProcessTime() + tl.getSetupTime());
			
			long diffs = tl.getProcessTime() - ts.getProcessTime()
					+ tl.getSetupTime() - oldsSetup;
			
			long diffl = tl.getProcessTime() - ts.getProcessTime()
			+ oldlSetup - ts.getSetupTime();
			
			if (diffl < 0 || diffs < 0)
				break;

			long gain = 0; 
			long loss = 0;
			
			for (int i = si; i < shortest.getTasks().size(); i++) {
				Task t = shortest.getTasks().get(i);
				long old = t.getFinishTime();
				t.setFinishTime(t.getFinishTime() + diffs);
				t.setStartTime(t.getStartTime() + diffs);

				if (t.getFinishTime() > t.getJob().getMapFinishTime()){
					//loss += (t.getFinishTime() - t.getJob().getMapFinishTime());
					t.getJob().setMapFinishTime(t.getFinishTime());
					
				}

			}
			for (int i = li; i < longest.getTasks().size(); i++) {
				Task t = longest.getTasks().get(i);
				long old = t.getFinishTime();
				t.setFinishTime(t.getFinishTime() - diffl);
				t.setStartTime(t.getStartTime() - diffl);

				if (old == t.getJob().getMapFinishTime()){
					t.getJob().setMapFinishTime(t.getFinishTime());
					//gain += (old - t.getJob().getMapFinishTime());
				}

			}
			//tl.setStartTime(shortest.getTasks().get(sp - 2).getFinishTime());
		   // tl.setFinishTime(tl.getStartTime() + tl.getProcessTime() + tl.getSetupTime());
		    
		    
		    if(ts.getFinishTime() > ts.getJob().getMapFinishTime())
		    {
		    	//loss += (ts.getFinishTime() - ts.getJob().getMapFinishTime());
		    	ts.getJob().setMapFinishTime(ts.getFinishTime());
		    	
		    }
		    if(tl.getFinishTime() > tl.getJob().getMapFinishTime()){
		    	//loss += (tl.getFinishTime() - tl.getJob().getMapFinishTime());
		    	tl.getJob().setMapFinishTime(tl.getFinishTime());
		    }
		    	
		    
			shortest.getTasks().add(si, tl);
			shortest.setCurFinishTime(shortest.getTasks().get(sp - 1).getFinishTime());
			longest.getTasks().add(li, ts);
			longest.setCurFinishTime(longest.getTasks().get(sr - 1).getFinishTime());
			
			Collections.sort(slots, new SlotComparator());
			
			//if(gain <= loss)
			count++;
			//else
				//break;

			
			 * long oldsSetup = ts.getSetupTime(); long oldlSetup =
			 * tl.getSetupTime();
			 * 
			 * schedule.calculateSetupTime(ts, longest);
			 * schedule.calculateSetupTime(tl, shortest);
			 * 
			 * long newl = ltime - tl.getProcessTime() - oldlSetup; long news =
			 * stime - ts.getProcessTime() - oldsSetup;
			 * 
			 * newl = Math.max(newl + ts.getProcessTime() + ts.getSetupTime(),
			 * ts.getJob().getMapFinishTime()); news = Math.max(news +
			 * tl.getProcessTime() + tl.getSetupTime(),
			 * tl.getJob().getMapFinishTime());
			 * 
			 * if(newl + news < ts.getJob().getMapFinishTime() +
			 * tl.getJob().getMapFinishTime()) { longest.getTasks().add(ts);
			 * shortest.getTasks().add(tl);
			 * 
			 * longest.setCurFinishTime(newl); shortest.setCurFinishTime(news);
			 * 
			 * tl.getJob().setMapFinishTime(news);
			 * ts.getJob().setMapFinishTime(Math.max(newl,
			 * ts.getJob().getMapFinishTime()));//tl and ts may be from same job
			 * 
			 * schedule.getMapTask2Slot().remove(ts.getTaskID());
			 * schedule.getMapTask2Slot().put(ts.getTaskID(),
			 * longest.getNodeID());
			 * schedule.getMapTask2Slot().remove(tl.getTaskID());
			 * schedule.getMapTask2Slot().put(tl.getTaskID(),
			 * shortest.getNodeID());
			 * 
			 * Collections.sort(slots, new SlotComparator()); count = 0; } else
			 * { //longest.getTasks().add(tl); shortest.getTasks().add(ts);
			 * //Collections.sort(slots, new SlotComparator());
			 * 
			 * //swap last two task Task last = tl; Task lsec =
			 * longest.getTasks().remove(sr - 2);
			 * 
			 * longest.getTasks().add(last); longest.getTasks().add(lsec);
			 * 
			 * if(longest.getCurFinishTime() ==
			 * last.getJob().getMapFinishTime())
			 * last.getJob().setMapFinishTime(longest.getCurFinishTime() -
			 * lsec.getProcessTime() - lsec.getSetupTime());
			 * 
			 * if(longest.getCurFinishTime() > lsec.getJob().getMapFinishTime())
			 * lsec.getJob().setMapFinishTime(longest.getCurFinishTime());
			 * 
			 * count++; }
			 
		}
		
	}*/
	
	@Override
	public void output() {
		System.out.println("The makespan of the schedule under backward heuristic method is: " + schedule.getMakespan());
		System.out.println("The relative quality of the solution is: " + 1.0 * (schedule.getMakespan() - schedule.lowerBound()) / schedule.lowerBound());
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
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Parameters p = new Parameters();
		//p.setP_io_rate(0.1);
		p.setP_jobs(50);
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
		
		RandomInstance ri = new RandomInstance(p, "1");
		RandomInstanceFile rf = new RandomInstanceFile(p, "G:\\Research\\Data\\MR_I_Jay_1.txt");
		try {
			Schedule s = ri.newInstance();
			//System.out.println(s.getLowerBound_1());
			//System.out.println(s.getLowerBound_2());
			System.out.println(s.getLowerBound());
			BackwardHeuristic bh = new BackwardHeuristic(s, "JR-LPT");
			bh.init();
			bh.start();
			bh.output();
			
			
			Schedule ss = rf.newInstance();
			//System.out.println(ss.getLowerBound_1());
			//System.out.println(ss.getLowerBound_2());
			System.out.println(ss.getLowerBound());
			GreedyHeuristic dm1 = new GreedyHeuristic(ss, "JR-LPT");
			dm1.init();
			dm1.start();
			dm1.output();
			
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
