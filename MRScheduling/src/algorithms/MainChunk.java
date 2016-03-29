
package algorithms;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.FormatFlagsConversionMismatchException;
import java.util.List;
import java.util.Random;

import javax.security.auth.login.FailedLoginException;

import data.Parameters;
import data.RandomInstanceFile;
import data.Tools;
import model.Job;
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
	
	//调用此方法的前提是对每一个list都用Schedule执行过一遍
	private class ListComparator implements Comparator<Object>{
		@SuppressWarnings("unchecked")
		@Override
		public int compare(Object o1, Object o2) {
			// TODO Auto-generated method stub
			List<Job> list1 = (List<Job>)o1;
			List<Job> list2 = (List<Job>)o2;
			
			long r1 = getTotalPenaltyCost(list1),r2 = getTotalPenaltyCost(list2);
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
        	//初始化资源信息
        	Tools.clearResources(schedule);
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
    			}
    		}
    		System.out.print(" reduce阶段完成时间:" + FinishTime);	
    		System.out.println("总惩罚代价: " + getTotalPenaltyCost(rJlist));
        	///////////////////////////////////
        }
    }
	
	//直接把joblist按顺序调度执行,返回惩罚代价
	public long runlist(Schedule schedule,List<Job> joblist){
		// TODO Auto-generated method stub
    	long currentTime = 0;
		List<Job> jlist = joblist;
		List<Job> rJlist = new ArrayList<Job>();
		
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
			}
		}

		currentTime = mapFinishTime;
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
			}
		}
		long penalty = getTotalPenaltyCost(rJlist);
		return penalty;
    	///////////////////////////////////	
	}
	public List<Job> des_reconstructure(Schedule schedule, List<Job> joblist, int d) throws Exception{
		int n = joblist.size();
		long t = System.currentTimeMillis();
        Random r = new Random(t);
        int remove_n = 1;
        List<Job> removeJobs = new ArrayList<Job>();
        for(int i = 0; i < d; i++){
            //一个0~n-1的随机数 
            int rn = r.nextInt(n + 1 - remove_n);
            removeJobs.add(joblist.get(rn));
            joblist.remove(rn);
            remove_n++;
        }
        Collections.reverse(removeJobs);
        for(int i = removeJobs.size() - 1; i >= 0; i--){
        	List<List<Job>> tepLists = new ArrayList<List<Job>>();
        	Job job = removeJobs.get(i);
        	removeJobs.remove(i);
        	for(int j = 0; j <= joblist.size(); j++){
        		List<Job> tepList = new ArrayList<Job>();
        		tepList = deepCopy(joblist);
        		//在j位置加入当前待处理作业
        		tepList.add(j,job);
        		//插入完成的list加入到list集合中
        		tepLists.add(tepList);
        	}
			List<List<Job>> listsForCheckBk = new ArrayList<List<Job>>();
        	for (List<Job> list : tepLists) {
        		Tools.clearJobInfo(list);
				Tools.clearJobTaskList(list);
				Tools.clearResources(schedule);
				runlist(schedule, list);
				List<Job> tList = deepCopy(list);
				listsForCheckBk.add(tList);
			}
        	//按照总惩罚代价从小到大排序
			Collections.sort(listsForCheckBk, new ListComparator());
			//更新待处理作业序列
			joblist = listsForCheckBk.get(0);
        }
        Tools.clearJobInfo(joblist);
        Tools.clearJobTaskList(joblist);
        Tools.clearResources(schedule);
		return joblist;
	}
	public int[] getN(int num){
		Random r = new Random(System.currentTimeMillis());
		int[] arr = new int[num];
		for(int i = 0; i < num; ++i){
			arr[i]=i;
		}
		for(int i = num - 1;i >= 0; --i){
			int temp = arr[i];
			int tepIndex = r.nextInt(num);
			arr[i] = arr[tepIndex];
			arr[tepIndex] = temp;
		}
		return arr;
	}
	public List<Job> iterative_improvement(Schedule schedule, List<Job> joblist) throws Exception{
		boolean improve = true;
		while(improve == true){
			improve = false;
			int length = joblist.size();
	        int[] ra = getN(length);
			for(int i = 0; i < length; i++){
				//joblist为是上一个最优作业序列
				List<Job> list1 = deepCopy(joblist);
				int pos = ra[i];
				Job job = list1.get(pos);
				list1.remove(pos);
				List<List<Job>> listsForCheck = new ArrayList<List<Job>>();
				
				for(int k = 0; k <= list1.size(); k++){
					List<Job> ltJobs = new ArrayList<Job>(list1);
					ltJobs.add(k, job);
					listsForCheck.add(ltJobs);
				}
				List<List<Job>> listsForCheckBk = new ArrayList<List<Job>>();
				for (List<Job> list2 : listsForCheck) {
					Tools.clearJobInfo(list2);
					Tools.clearJobTaskList(list2);
					Tools.clearResources(schedule);
					runlist(schedule, list2);
					List<Job> tList = deepCopy(list2);
					listsForCheckBk.add(tList);
				}
				Collections.sort(listsForCheckBk, new ListComparator());
				//搜索后的结果
				List<Job> ll = listsForCheckBk.get(0);
				if(getTotalPenaltyCost(ll) < getTotalPenaltyCost(joblist)){
					joblist = ll;
					improve = true;
				}
			}
		}
		return joblist;
	}
	public long execute(Schedule schedule,List<Job> joblist) throws Exception {
		// TODO Auto-generated method stub
		//初始化
		List<Job> jlist = GetJobSequence(schedule,joblist);
		System.out.print("初始作业序列: ");
		printAlist(jlist);
		Tools.clearResources(schedule);
		Tools.clearJobInfo(jlist);
		Tools.clearJobTaskList(jlist);
		//初始化部分
		//用于初始化温度
		double aJobProcessTime = 0;
		for (Job job : jlist) {
			double mTaskExeTime = 0,rTaskExetime = 0;
			for (Task task : job.getMaps()) {
				mTaskExeTime += task.getProcessTime();
			}
			for (Task task : job.getMaps()) {
				rTaskExetime += task.getProcessTime();
			}
			mTaskExeTime /= job.getMaps().size();
			rTaskExetime /= job.getReduces().size();
			aJobProcessTime += mTaskExeTime;
			aJobProcessTime += rTaskExetime;
		}
		
		//可调参数
		int adjustT = 50;
		//初始化温度
		long Temperature = (long)(adjustT *(aJobProcessTime)/(joblist.size() * 2 * 10));
		long iterated_generations = 0;
		long max_generations = 5000000;
		//最小的代价列表PI b
		List<Job> listb = jlist;
		while(iterated_generations <= max_generations){
			iterated_generations++;
			//PI'								PI
			Tools.clearJobInfo(jlist);
			Tools.clearJobTaskList(jlist);
			Tools.clearResources(schedule);
			List<Job> last_solution = deepCopy(jlist);
			//解构和重构部分
			last_solution = des_reconstructure(schedule, last_solution, 2);
			//局部搜索部分 获得PI''
			List<Job> after_localsearch = iterative_improvement(schedule, deepCopy(last_solution));
			runlist(schedule, after_localsearch);
			long penalty1 = getTotalPenaltyCost(after_localsearch);
			Tools.clearResources(schedule);
			runlist(schedule, jlist);
			long penalty2 = getTotalPenaltyCost(jlist);
			if(penalty1 < penalty2){
				jlist = after_localsearch;
				if(getTotalPenaltyCost(jlist) < getTotalPenaltyCost(listb)){
					listb = jlist;
				}
			//以一定的概率接受当前的较差解
			}else if(Math.random() < Math.pow(Math.E, -(penalty1 - penalty2)/Temperature)){
				last_solution = after_localsearch;
			}
			//降低温度
			Temperature *= 0.8;
		}
			
		long penalty = getTotalPenaltyCost(listb);
		return penalty;
    	///////////////////////////////////	
	}
	//找到能完成的序列
	public List<List<Job>> getCanFinish(Schedule schedule,List<List<Job>> ls){
			int count = 0;
			List<List<Job>> lists = new ArrayList<List<Job>>(ls);
			List<List<Job>> resultLists = new ArrayList<List<Job>>();
			boolean flag = true;
			if(lists.isEmpty())
				return null;
			
			for (List<Job> joblist : lists) {
				//把当前作业序列执行一遍
				Tools.clearJobInfo(joblist);
	        	Tools.clearJobTaskList(joblist);
	        	Tools.clearResources(schedule);
				runlist(schedule,joblist);
				for (Job job : joblist) {
					//拖期完成的情况
					if(job.getFinishTime() > job.getDeadline()){
						flag = false;
					}
				}
				if(flag == true){
					resultLists.add(new ArrayList<Job>(joblist));
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
	public List<Job> GetJobSequence(Schedule schedule,List<Job> joblist) throws Exception{
		Job ji = null;
		for(int i = 0; i < joblist.size(); i++)
		{
			ji = joblist.get(i);
			ji.setSortKey(ji.getDeadline());
		}
		Collections.sort(joblist, new JobComparator());
		List<List<Job>> tepLists = new ArrayList<List<Job>>();
		List<List<Job>> listsA = new ArrayList<List<Job>>();
		List<Job> listB = new ArrayList<Job>(joblist);
		
		List<Job> tep0 = new ArrayList<Job>();
		tep0.add(listB.get(0));
		listsA.add(tep0);
		listB.remove(0);
		boolean flag = true;
		while (!listB.isEmpty()) {	
			Job toBeInserted = listB.get(0);
			listB.remove(0);
			for(List<Job> list : listsA){
				for(int i = 0; i <= list.size(); i++){
					List<Job> lst = new ArrayList<Job>(list);
					lst.add(i, toBeInserted);
					tepLists.add(lst);
				}	
			}
			//llJobs中的作业执行信息已经被更新到作业中
			List<List<Job>> llJobs = getCanFinish(schedule,tepLists);
			if(llJobs != null){
				listsA = llJobs;	
			}else{
				flag = false;
				break;
			}			
			tepLists.clear();
		}
		if(listsA.isEmpty() || flag == false){
			return minimizePenaltyCost(schedule,joblist);
		}else{
			return getMinTCList(listsA);
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
			Tools.clearResources(schedule);
			Tools.clearJobTaskList(joblist);
			Tools.clearJobInfo(joblist);
			Schedule step = new Schedule(schedule);
			runlist(step,joblist);
			for (Job job : joblist) {
				if(job.getFinishTime() > job.getDeadline()){
					flag = false;
				}
			}
			if(flag == true){
				resultLists.add(new ArrayList<Job>(joblist));
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
	public List<Job> minimizePenaltyCost(Schedule schedule, List<Job> joblist) throws Exception{
    	Tools.clearResources(schedule);
		Job ji = null;
		for(int i = 0; i < joblist.size(); i++)
		{
			ji = joblist.get(i);
			ji.setSortKey(ji.getDeadline());
		}
		Collections.sort(joblist, new JobComparator());
		List<Job> SES = new ArrayList<Job>();
		//SES初始状态为只包含J0的列表集合
		SES.add(joblist.get(0));
		for (int j= 1; j < joblist.size(); j++) {
			Job job = joblist.get(j);
			List<Job> list = SES;
			//待检查列表集合
			List<List<Job>> listsForCheck = new ArrayList<List<Job>>();
			for(int k = 0; k <= list.size(); k++){
				List<Job> ltJobs = new ArrayList<Job>(list);
				ltJobs.add(k, job);
				listsForCheck.add(ltJobs);
			}
			List<List<Job>> listsForCheckBk = new ArrayList<List<Job>>();
			//把列表中的每一个序列执行一遍
			for (List<Job> list2 : listsForCheck) {
				Tools.clearJobInfo(list2);
				Tools.clearJobTaskList(list2);
				Tools.clearResources(schedule);
				runlist(schedule, list2);
				List<Job> tList = deepCopy(list2);
				listsForCheckBk.add(tList);
			}
			
			//按照总惩罚代价从小到大排序
			Collections.sort(listsForCheckBk, new ListComparator());
			//打印总惩罚代价
			for (List<Job> list2 : listsForCheckBk) {
				printAlist(list2);
				System.out.print(":");
				System.out.println(getTotalPenaltyCost(list2));
			}
			SES = listsForCheckBk.get(0);
		}
		return SES;
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
			totalPC += ((job.getFinishTime() - job.getDeadline()) > 0 ? (job.getFinishTime() - job.getDeadline()) : 0);
		}
		return totalPC;
	}
	//打印ArrayList
	public void printAlist(List<Job> joblist){
		for (Job job : joblist) {
			System.out.print(job.getJobID() + " ");
		}
	}
	public void start(Schedule schedule) throws Exception{
		execute(schedule,schedule.getJobs());
	}
	public void output() {
		// TODO Auto-generated method stub
		
	}
	public List<Job> deepCopy(List<Job> src) throws Exception {             
	    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();             
	    ObjectOutputStream out = new ObjectOutputStream(byteOut);             
	    out.writeObject(src);                    
	    ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());             
	    ObjectInputStream in =new ObjectInputStream(byteIn);             
	    @SuppressWarnings("unchecked")
		List<Job> dest = (List<Job>)in.readObject();             
	    return dest;         
	}   
	
	public static void main(String[] args)throws Exception{
		Parameters p = new Parameters();
		p.setP_io_rate(0.1);

//		p.setP_rack(10);
//		p.setP_node(100);
//		p.setP_map_slot(4);
//		p.setP_reduce_slot(2);
		
		
		p.setP_rack(2);
		p.setP_node(4);
		p.setP_map_slot(2);
		p.setP_reduce_slot(1);
		
		
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
		
		RandomInstanceFile rf = new RandomInstanceFile(p, "..\\..\\TestData\\");
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