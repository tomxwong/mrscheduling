
package algorithms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import data.Parameters;
import data.RandomInstanceFile;
import data.Tools;
import model.Cluster;
import model.Job;
import model.JobComparator;
import model.Schedule;
import model.Task;


public class MainChunk{
	public class taskComparator implements Comparator<Object>{
		public int compare(Object o1, Object o2)
	    {
	         Task t1 = (Task)o1;
	         Task t2 = (Task)o2;
	         if(t1.getProcessTime() > t2.getProcessTime()) 
	        	 return -1;
	         else if(t1.getProcessTime() == t2.getProcessTime()) 
	        	 return 0;
	         return 1;
	    }
	}
	private class JobComparator implements Comparator<Object>{
		public int compare(Object o1, Object o2)
	    {
	         Job j1 = (Job)o1;
	         Job j2 = (Job)o2;
	         if(j1.getSortKey() > j2.getSortKey()) 
	        	 return 1;
	         else if( j1.getSortKey() == j2.getSortKey()) 
	        	 return 0;
	         return -1;
	    }
	}
	private class ListComparator implements Comparator<Object>{
		@SuppressWarnings("unchecked")
		@Override
		public int compare(Object o1, Object o2) {
			// TODO Auto-generated method stub
			List<Job> list1 = (List<Job>)o1;
			List<Job> list2 = (List<Job>)o2;
			long r1 = calculateTotalPenaltyCost(list1),r2 = calculateTotalPenaltyCost(list2);
			if(r1 > r2){
				return 1;
			}else if(r1 == r2){
				return 0;
			}else{
				return -1;
			}
		}
	}
	public MainChunk(Schedule schedule) {
		super();
	}
	
	public void init() {
		// TODO Auto-generated method stub
		//Tools.clearSchedule(schedule);		
	}
	public void permute(java.util.List<Job> arr, int k,Schedule schedule){
        for(int i = k; i < arr.size(); i++){
            java.util.Collections.swap(arr, i, k);
            permute(arr, k+1,schedule);
            java.util.Collections.swap(arr, k, i);
        }
        if (k == arr.size() -1){
            ///////////////////////////////////
        	Tools.clearSchedule(schedule);
        	long currentTime = 0;
    		List<Job> jlist = new ArrayList<Job>(arr);
    		List<Job> rJlist = new ArrayList<Job>();
    		System.out.print("作业序列: ");
    		printAlist(jlist);
    		//map阶段
    		//maxMapFinishTime用于记录所有作业的最大完成时间
    		long mapFinishTime = Integer.MAX_VALUE;
    		
    		boolean bFirstFinishMapJob = false;
			//记录Map最大完成时间
			long mFinishTime = 0;
    		for (int i = 0; i < jlist.size(); i++) {
    			Job job = jlist.get(i);

    			Collections.sort(job.getMaps(), new taskComparator());
    			for (int j = 0; j < job.getMaps().size(); j++){
    				Task task = job.getMaps().get(j);
    				//如果已经处理则继续下一个任务
    				if(task.isProcessed() == true)
    					continue;
    				//按Job顺序调度任务序列，前面的没执行完 则后面的不执行
    				long assignResult = schedule.assignTaskNode(task,currentTime);
    				while(assignResult == -1){
    					assignResult = schedule.assignTaskNode(task,currentTime);
    					currentTime ++;
    				}
    				//更新任务结束时间
    				if(task.getFinishTime() > mFinishTime){
    					mFinishTime = task.getFinishTime();
    				}
    				//如果是Map的最后一个任务，则把此作业加入到reduce队列中
    				if(job.getMaps().indexOf(task) == job.getMapNum() - 1){
    					job.setMapFinishTime(mFinishTime);
    	
    					//第一次完成的Map作业的完成时间成为reduce的开始时间
    					if(bFirstFinishMapJob == false){
    						mapFinishTime = mFinishTime;
    						bFirstFinishMapJob = true;
    					}
    					rJlist.add(job);
    					//恢复mFinishTime
    					mFinishTime = 0;
    				}
    				i = -1;
    				break;
    			}
    		}
  		
    		currentTime = mapFinishTime;
    		System.out.print("reduce阶段开始时间 :" + currentTime);
    		//reduce阶段
    		//记录reduce阶段结束时间
			long rFinishTime = 0;
			
    		long FinishTime = 0;
    		for (int i = 0; i < rJlist.size(); i++) {
    			Job job = rJlist.get(i);
    			Collections.sort(job.getReduces(), new taskComparator());
    			
    			for (int j = 0; j < job.getReduces().size(); j++){
    				Task task = job.getReduces().get(j);
    				//如果已经处理则继续下一个任务
    				if(task.isProcessed() == true)
    					continue;
    				//按Job顺序调度任务序列，前面的没执行完 则后面的不执行
    				long assignResult = schedule.assignTaskNode(task,currentTime);
    				while(assignResult == -1){
    					assignResult = schedule.assignTaskNode(task,currentTime);
    					currentTime ++;
    				}
    				//更新reduce结束时间
    				if(task.getFinishTime() > rFinishTime){
    					rFinishTime = task.getFinishTime();
    				}
    				//如果是Map的最后一个任务，则把此作业加入到reduce队列中
    				if(job.getReduces().indexOf(task) == job.getReduceNum() - 1){
    					job.setReduceFinishTime(rFinishTime);
    					job.setFinishTime(rFinishTime);
    					//恢复rFinishTime
    					rFinishTime = 0;
    					if(job.getFinishTime() > FinishTime){
    						FinishTime = job.getFinishTime();
    					}
    				}
    				i = -1;
    				break;
    			}
    		}
    		System.out.print(" reduce阶段完成时间:" + FinishTime);	
    		System.out.println("总惩罚代价: " + getTotalPenaltyCost(rJlist));
        	///////////////////////////////////
        }
    }
	public long schedule(Schedule schedule,List<Job> joblist) {
		// TODO Auto-generated method stub
    	long currentTime = 0;
		List<Job> jlist = GetJobSequence(schedule,joblist);
		List<Job> rJlist = new ArrayList<Job>();
		System.out.print("新作业序列: ");
		printAlist(jlist);
		//map阶段
		//maxMapFinishTime用于记录所有作业的最大完成时间
		long mapFinishTime = Integer.MAX_VALUE;
		
		boolean bFirstFinishMapJob = false;
		//记录Map最大完成时间
		long mFinishTime = 0;
		for (int i = 0; i < jlist.size(); i++) {
			Job job = jlist.get(i);

			Collections.sort(job.getMaps(), new taskComparator());
			for (int j = 0; j < job.getMaps().size(); j++){
				Task task = job.getMaps().get(j);
				//如果已经处理则继续下一个任务
				if(task.isProcessed() == true)
					continue;
				//按Job顺序调度任务序列，前面的没执行完 则后面的不执行
				long assignResult = schedule.assignTaskNode(task,currentTime);
				while(assignResult == -1){
					assignResult = schedule.assignTaskNode(task,currentTime);
					currentTime ++;
				}
				//更新任务结束时间
				if(task.getFinishTime() > mFinishTime){
					mFinishTime = task.getFinishTime();
				}
				//如果是Map的最后一个任务，则把此作业加入到reduce队列中
				if(job.getMaps().indexOf(task) == job.getMapNum() - 1){
					job.setMapFinishTime(mFinishTime);
	
					//第一次完成的Map作业的完成时间成为reduce的开始时间
					if(bFirstFinishMapJob == false){
						mapFinishTime = mFinishTime;
						bFirstFinishMapJob = true;
					}
					rJlist.add(job);
					//恢复mFinishTime
					mFinishTime = 0;
				}
				i = -1;
				break;
			}
		}

		currentTime = mapFinishTime;
		System.out.print("reduce阶段开始时间 :" + currentTime);
		//reduce阶段
		//记录reduce阶段结束时间
		long rFinishTime = 0;
		
		long FinishTime = 0;
		for (int i = 0; i < rJlist.size(); i++) {
			Job job = rJlist.get(i);
			Collections.sort(job.getReduces(), new taskComparator());
			
			for (int j = 0; j < job.getReduces().size(); j++){
				Task task = job.getReduces().get(j);
				//如果已经处理则继续下一个任务
				if(task.isProcessed() == true)
					continue;
				//按Job顺序调度任务序列，前面的没执行完 则后面的不执行
				long assignResult = schedule.assignTaskNode(task,currentTime);
				while(assignResult == -1){
					assignResult = schedule.assignTaskNode(task,currentTime);
					currentTime ++;
				}
				//更新reduce结束时间
				if(task.getFinishTime() > rFinishTime){
					rFinishTime = task.getFinishTime();
				}
				//如果是Map的最后一个任务，则把此作业加入到reduce队列中
				if(job.getReduces().indexOf(task) == job.getReduceNum() - 1){
					job.setReduceFinishTime(rFinishTime);
					job.setFinishTime(rFinishTime);
					//恢复rFinishTime
					rFinishTime = 0;
					if(job.getFinishTime() > FinishTime){
						FinishTime = job.getFinishTime();
					}
				}
				i = -1;
				break;
			}
		}
		System.out.print(" reduce阶段完成时间:" + FinishTime);	
	
		long penalty = getTotalPenaltyCost(rJlist);
		joblist = rJlist;
		return penalty;
    	///////////////////////////////////	
	}
	//找到能完成的序列
	public List<List<Job>> getCanFinish(Schedule schedule,List<List<Job>> lists){
			int count = 0;
			List<List<Job>> resultLists = new ArrayList<List<Job>>();

			boolean flag = true;
			if(lists.isEmpty())
				return null;
			
			for (List<Job> joblist : lists) {
				//把当前作业序列执行一遍
				Schedule step = new Schedule(schedule);
				schedule(step,joblist);
				for (Job job : joblist) {
					//拖期完成的情况
					if(job.getFinishTime() > job.getDeadline()){
						flag = false;
					}
				}
				if(flag == true){
					resultLists.add(joblist);
					count++;
				}else{
					flag = true;
				}
			}
			if(count == 0)
				return null;
			return resultLists;
	}
	//找到最优序列
	public List<Job> GetJobSequence(Schedule schedule,List<Job> joblist){
		Job ji = null;
		for(int i = 0; i < joblist.size(); i++)
		{
			ji = joblist.get(i);
			ji.setSortKey(ji.getDeadline());
		}
		Collections.sort(joblist, new JobComparator());
		List<List<Job>> tepLists = new ArrayList<List<Job>>();
		List<List<Job>> listLeftJobsSequences = new ArrayList<List<Job>>();
		List<Job> listRightJobs = new ArrayList<Job>(joblist);
		List<Job> tep0 = new ArrayList<Job>();
		tep0.add(listRightJobs.get(0));
		listLeftJobsSequences.add(tep0);
		listRightJobs.remove(0);
		boolean flag = true;
		while (!listRightJobs.isEmpty()) {	
			Job toBeInserted = listRightJobs.get(0);
			listRightJobs.remove(0);
			for(List<Job> list : listLeftJobsSequences){
				for(int i = 0; i <= list.size(); i++){
					List<Job> lst = new ArrayList<Job>(list);
					lst.add(i, toBeInserted);
					tepLists.add(lst);
				}	
			}
			List<List<Job>> llJobs = getCanFinish(schedule,tepLists);
			if(llJobs != null){
				listLeftJobsSequences = llJobs;	
			}else{
				flag = false;
				break;
			}			
			tepLists.clear();
		}
		if(listLeftJobsSequences.isEmpty() || flag == false){
			return minimizePenaltyCost(schedule,joblist);
		}else{
			return getMinTCList(listLeftJobsSequences);
		}
	}
	//找到能完成且TC最小序列
	public List<Job> getCanFinishMinTC(Schedule schedule,List<List<Job>> lists){
		int count = 0;
		List<List<Job>> resultLists = new ArrayList<List<Job>>();

		boolean flag = true;
		if(lists.isEmpty())
			return null;
		for (List<Job> joblist : lists) {
			Schedule step = new Schedule(schedule);
			schedule(step,joblist);
			for (Job job : joblist) {
				if(job.getFinishTime() > job.getDeadline()){
					flag = false;
				}
			}
			if(flag == true){
				resultLists.add(joblist);
				count++;
			}else{
				flag = true;
			}
		}
		if(flag == false && count == 0)
			return null;
		return getMinTCList(resultLists);
	}
	//无法找到ES处理方法
	public List<Job> minimizePenaltyCost(Schedule schedule, List<Job> joblist){
		
		Job ji = null;
		for(int i = 0; i < joblist.size(); i++)
		{
			ji = joblist.get(i);
			ji.setSortKey(ji.getDeadline());
		}
		Collections.sort(joblist, new JobComparator());
		
		List<Job> jlist = new ArrayList<Job>();
		jlist.add(joblist.get(0));
		List<List<Job>> SES = new ArrayList<List<Job>>();
		//SES初始状态为只包含J0的列表集合
		SES.add(jlist);
		
		for (Job job : joblist) {
			//对SES中的每一个列表用当前job处理
			for(int j = 0; j < SES.size(); j++){
				
				//SES中的每一个列表
				List<Job> list = SES.get(j);
				boolean bProcessed = false;
					if(!list.contains(job)){
						//待检查列表集合
						List<List<Job>> listsForCheck = new ArrayList<List<Job>>();
						
						for(int i = 0; i <= list.size(); i++){
							bProcessed = true;
							List<Job> ltJobs = new ArrayList<Job>(list);
							ltJobs.add(i, job);
							listsForCheck.add(ltJobs);
						}
						//此处因排序导致listsForCheck中列表完成时间不正确，但顺序正确
						Collections.sort(listsForCheck, new ListComparator());
						int nMin = getNList(schedule,listsForCheck);												
						SES.addAll(listsForCheck.subList(0, nMin));
					}
					if(bProcessed == true){
						SES.remove(j);
							j --;
					}				
			}
		}
		return getMinMakeSpanList(SES);
	}
	public List<Job> getMinMakeSpanList(List<List<Job>> lists){
		
		return null;
	}
	//找到TC最小的
	public List<Job> getMinTCList(List<List<Job>> lists){
			int index = -1;
			long minTC = Integer.MAX_VALUE;
			for (List<Job> list : lists) {
				long tpc = 0;
				for (Job job : list) {
					tpc += ((job.getFinishTime() - job.getDeadline()) >= 0 ? (job.getFinishTime() - job.getDeadline()) : 0);
				}
				if(tpc < minTC){
					minTC = tpc;
					index = lists.indexOf(list);
				}
			}
			return lists.get(index);
	}
	
	//计算当前作业顺序的Total TimeCost
	public long getTotalPenaltyCost(List<Job> joblist){
		long totalPC = 0;
		for (Job job : joblist) {
			if(job.getFinishTime() > job.getDeadline()){
				totalPC += (job.getFinishTime() - job.getDeadline());
			}else{
				totalPC += 0;
			}
		}
		return totalPC;
	}
	//找到最前n个相同的元素
	public int getNList(Schedule schedule,List<List<Job>> lists){
		long minPenalty = schedule(schedule,lists.get(0));
		int index = 1;
		for (int i = 1; i < lists.size(); i++) {
			long curPenalty = schedule(schedule,lists.get(i));
			if(curPenalty == minPenalty){
				index ++;
			}
		}
		return index;
	}
	
	
	//判断当前job序列是否能够全部按时完成
	public boolean canFinishOnTime(Schedule schedule,List<Job> joblist){
		Schedule step = new Schedule(schedule);
		schedule(step,joblist);
		for (Job job : joblist) {
			if(job.getFinishTime() > job.getDeadline()){
				return false;
			}
		}
		return true;
	}
	
	//打印ArrayList
	public void printAlist(List<Job> joblist){
		for (Job job : joblist) {
			System.out.print(job.getJobID() + " ");
		}
	}
	public void start(Schedule schedule){
		schedule(schedule,schedule.getJobs());
	}

	public void output() {
		// TODO Auto-generated method stub
		
	}
	public static void main(String[] args){
		Parameters p = new Parameters();
		p.setP_io_rate(0.1);

		p.setP_rack(10);
		p.setP_node(100);
		p.setP_map_slot(4);
		p.setP_reduce_slot(2);
		
		p.setP_map_num_miu(15);
		p.setP_map_num_sig(55);
		p.setP_map_dura_miu(5);
		p.setP_map_dura_sig(20);
		
		p.setP_reduce_num_miu(2);
		p.setP_reduce_num_sig(14);
		p.setP_reduce_dura_miu(10);
		p.setP_reduce_dura_sig(30);
		
		//extra add
		p.setP_job_deadline_miu(200);
		p.setP_job_deadline_sigma(100);
		
		RandomInstanceFile rf = new RandomInstanceFile(p, "..\\TestData\\");
		try {
			Schedule s = rf.newInstance();
			s.setIoRate(p.getP_io_rate());
			MainChunk mc = new MainChunk(s);
			//mc.init();
			//mc.output();
			//mc.permute(mc.schedule.getJobs(), 0,s);
			mc.start(s);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}	
	
}