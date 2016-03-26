
package algorithms;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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
	
	//���ô˷�����ǰ���Ƕ�ÿһ��list����Scheduleִ�й�һ��
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
        	//��ʼ����Դ��Ϣ
        	Tools.clearResources(schedule);
        	long currentTime = 0;
    		List<Job> jlist = new ArrayList<Job>(arr);
    		List<Job> rJlist = new ArrayList<Job>();
    		System.out.print("��ҵ����: ");
    		printAlist(jlist);
    		//map�׶�
    		//maxMapFinishTime���ڼ�¼������ҵ��������ʱ��
    		long mapFinishTime = Integer.MAX_VALUE;
    		
    		boolean bFirstFinishMapJob = false;
			//��¼Map������ʱ��
			long mFinishTime = 0;
    		for (int i = 0; i < jlist.size(); i++) {
    			Job job = jlist.get(i);

    			Collections.sort(job.getMaps(), new taskComparator());
    			for (int j = 0; j < job.getMaps().size(); j++){
    				Task task = job.getMaps().get(j);
    				//����Ѿ������������һ������
    				if(task.isProcessed() == true)
    					continue;
    				//��Job˳������������У�ǰ���ûִ���� �����Ĳ�ִ��
    				long assignResult = schedule.assignTaskNode(task,currentTime);
    				while(assignResult == -1){
    					assignResult = schedule.assignTaskNode(task,currentTime);
    					currentTime ++;
    				}
    				//�����������ʱ��
    				if(task.getFinishTime() > mFinishTime){
    					mFinishTime = task.getFinishTime();
    				}
    				//�����Map�����һ��������Ѵ���ҵ���뵽reduce������
    				if(job.getMaps().indexOf(task) == job.getMapNum() - 1){
    					job.setMapFinishTime(mFinishTime);
    	
    					//��һ����ɵ�Map��ҵ�����ʱ���Ϊreduce�Ŀ�ʼʱ��
    					if(bFirstFinishMapJob == false){
    						mapFinishTime = mFinishTime;
    						bFirstFinishMapJob = true;
    					}
    					rJlist.add(job);
    					//�ָ�mFinishTime
    					mFinishTime = 0;
    				}
    				i = -1;
    				break;
    			}
    		}
  		
    		currentTime = mapFinishTime;
    		System.out.print("reduce�׶ο�ʼʱ�� :" + currentTime);
    		//reduce�׶�
    		//��¼reduce�׶ν���ʱ��
			long rFinishTime = 0;
			
    		long FinishTime = 0;
    		for (int i = 0; i < rJlist.size(); i++) {
    			Job job = rJlist.get(i);
    			Collections.sort(job.getReduces(), new taskComparator());
    			
    			for (int j = 0; j < job.getReduces().size(); j++){
    				Task task = job.getReduces().get(j);
    				//����Ѿ������������һ������
    				if(task.isProcessed() == true)
    					continue;
    				//��Job˳������������У�ǰ���ûִ���� �����Ĳ�ִ��
    				long assignResult = schedule.assignTaskNode(task,currentTime);
    				while(assignResult == -1){
    					assignResult = schedule.assignTaskNode(task,currentTime);
    					currentTime ++;
    				}
    				//����reduce����ʱ��
    				if(task.getFinishTime() > rFinishTime){
    					rFinishTime = task.getFinishTime();
    				}
    				//�����Map�����һ��������Ѵ���ҵ���뵽reduce������
    				if(job.getReduces().indexOf(task) == job.getReduceNum() - 1){
    					job.setReduceFinishTime(rFinishTime);
    					job.setFinishTime(rFinishTime);
    					//�ָ�rFinishTime
    					rFinishTime = 0;
    					if(job.getFinishTime() > FinishTime){
    						FinishTime = job.getFinishTime();
    					}
    				}
    				i = -1;
    				break;
    			}
    		}
    		System.out.print(" reduce�׶����ʱ��:" + FinishTime);	
    		System.out.println("�ܳͷ�����: " + getTotalPenaltyCost(rJlist));
        	///////////////////////////////////
        }
    }
	
	//ֱ�Ӱ�joblist��˳�����ִ��
	public List<Job> runlist(Schedule schedule,List<Job> joblist){
		// TODO Auto-generated method stub
    	long currentTime = 0;
		List<Job> jlist = joblist;
		List<Job> rJlist = new ArrayList<Job>();
		
		//map�׶�
		//maxMapFinishTime���ڼ�¼������ҵ��������ʱ��
		long mapFinishTime = Integer.MAX_VALUE;
		boolean bFirstFinishMapJob = false;
		//��¼Map������ʱ��
		long mFinishTime = 0;
		for (int i = 0; i < jlist.size(); i++) {
			Job job = jlist.get(i);

			Collections.sort(job.getMaps(), new taskComparator());
			for (int j = 0; j < job.getMaps().size(); j++){
				Task task = job.getMaps().get(j);
				//����Ѿ������������һ������
				if(task.isProcessed() == true)
					continue;
				//��Job˳������������У�ǰ���ûִ���� �����Ĳ�ִ��
				long assignResult = schedule.assignTaskNode(task,currentTime);
				while(assignResult == -1){
					assignResult = schedule.assignTaskNode(task,currentTime);
					currentTime ++;
				}
				//�����������ʱ��
				if(task.getFinishTime() > mFinishTime){
					mFinishTime = task.getFinishTime();
				}
				//�����Map�����һ��������Ѵ���ҵ���뵽reduce������
				if(job.getMaps().indexOf(task) == job.getMapNum() - 1){
					job.setMapFinishTime(mFinishTime);
	
					//��һ����ɵ�Map��ҵ�����ʱ���Ϊreduce�Ŀ�ʼʱ��
					if(bFirstFinishMapJob == false){
						mapFinishTime = mFinishTime;
						bFirstFinishMapJob = true;
					}
					rJlist.add(job);
					//�ָ�mFinishTime
					mFinishTime = 0;
				}
			}
		}

		currentTime = mapFinishTime;
		//reduce�׶�
		//��¼reduce�׶ν���ʱ��
		long rFinishTime = 0;
		
		long FinishTime = 0;
		for (int i = 0; i < rJlist.size(); i++) {
			Job job = rJlist.get(i);
			Collections.sort(job.getReduces(), new taskComparator());
			
			for (int j = 0; j < job.getReduces().size(); j++){
				Task task = job.getReduces().get(j);
				//����Ѿ������������һ������
				if(task.isProcessed() == true)
					continue;
				//��Job˳������������У�ǰ���ûִ���� �����Ĳ�ִ��
				long assignResult = schedule.assignTaskNode(task,currentTime);
				while(assignResult == -1){
					assignResult = schedule.assignTaskNode(task,currentTime);
					currentTime ++;
				}
				//����reduce����ʱ��
				if(task.getFinishTime() > rFinishTime){
					rFinishTime = task.getFinishTime();
				}
				//�����Map�����һ��������Ѵ���ҵ���뵽reduce������
				if(job.getReduces().indexOf(task) == job.getReduceNum() - 1){
					job.setReduceFinishTime(rFinishTime);
					job.setFinishTime(rFinishTime);
					//�ָ�rFinishTime
					rFinishTime = 0;
					if(job.getFinishTime() > FinishTime){
						FinishTime = job.getFinishTime();
					}
				}
			}
		}
		return rJlist;
    	///////////////////////////////////	
	}
	public long execute(Schedule schedule,List<Job> joblist) throws Exception {
		// TODO Auto-generated method stub
    	long currentTime = 0;
		List<Job> jlist = GetJobSequence(schedule,joblist);
		List<Job> rJlist = new ArrayList<Job>();
		System.out.print("����ҵ����: ");
		printAlist(jlist);
		//map�׶�
		//maxMapFinishTime���ڼ�¼������ҵ��������ʱ��
		long mapFinishTime = Integer.MAX_VALUE;
		
		boolean bFirstFinishMapJob = false;
		//��¼Map������ʱ��
		long mFinishTime = 0;
		for (int i = 0; i < jlist.size(); i++) {
			Job job = jlist.get(i);

			Collections.sort(job.getMaps(), new taskComparator());
			for (int j = 0; j < job.getMaps().size(); j++){
				Task task = job.getMaps().get(j);
				//����Ѿ������������һ������
				if(task.isProcessed() == true)
					continue;
				//��Job˳������������У�ǰ���ûִ���� �����Ĳ�ִ��
				long assignResult = schedule.assignTaskNode(task,currentTime);
				while(assignResult == -1){
					assignResult = schedule.assignTaskNode(task,currentTime);
					currentTime ++;
				}
				//�����������ʱ��
				if(task.getFinishTime() > mFinishTime){
					mFinishTime = task.getFinishTime();
				}
				//�����Map�����һ��������Ѵ���ҵ���뵽reduce������
				if(job.getMaps().indexOf(task) == job.getMapNum() - 1){
					job.setMapFinishTime(mFinishTime);
	
					//��һ����ɵ�Map��ҵ�����ʱ���Ϊreduce�Ŀ�ʼʱ��
					if(bFirstFinishMapJob == false){
						mapFinishTime = mFinishTime;
						bFirstFinishMapJob = true;
					}
					rJlist.add(job);
					//�ָ�mFinishTime
					mFinishTime = 0;
				}
			}
		}

		currentTime = mapFinishTime;
		System.out.print("reduce�׶ο�ʼʱ�� :" + currentTime);
		//reduce�׶�
		//��¼reduce�׶ν���ʱ��
		long rFinishTime = 0;
		
		long FinishTime = 0;
		for (int i = 0; i < rJlist.size(); i++) {
			Job job = rJlist.get(i);
			Collections.sort(job.getReduces(), new taskComparator());
			
			for (int j = 0; j < job.getReduces().size(); j++){
				Task task = job.getReduces().get(j);
				//����Ѿ������������һ������
				if(task.isProcessed() == true)
					continue;
				//��Job˳������������У�ǰ���ûִ���� �����Ĳ�ִ��
				long assignResult = schedule.assignTaskNode(task,currentTime);
				while(assignResult == -1){
					assignResult = schedule.assignTaskNode(task,currentTime);
					currentTime ++;
				}
				//����reduce����ʱ��
				if(task.getFinishTime() > rFinishTime){
					rFinishTime = task.getFinishTime();
				}
				//�����Map�����һ��������Ѵ���ҵ���뵽reduce������
				if(job.getReduces().indexOf(task) == job.getReduceNum() - 1){
					job.setReduceFinishTime(rFinishTime);
					job.setFinishTime(rFinishTime);
					//�ָ�rFinishTime
					rFinishTime = 0;
					if(job.getFinishTime() > FinishTime){
						FinishTime = job.getFinishTime();
					}
				}
			}
		}
		System.out.print(" reduce�׶����ʱ��:" + FinishTime);	
	
		long penalty = getTotalPenaltyCost(rJlist);
		joblist = rJlist;
		return penalty;
    	///////////////////////////////////	
	}
	//�ҵ�����ɵ�����
	public List<List<Job>> getCanFinish(Schedule schedule,List<List<Job>> ls){
			int count = 0;
			List<List<Job>> lists = new ArrayList<List<Job>>(ls);
			List<List<Job>> resultLists = new ArrayList<List<Job>>();
			boolean flag = true;
			if(lists.isEmpty())
				return null;
			
			for (List<Job> joblist : lists) {
				//�ѵ�ǰ��ҵ����ִ��һ��
				Tools.clearJobInfo(joblist);
	        	Tools.clearJobTaskList(joblist);
	        	Tools.clearResources(schedule);
				runlist(schedule,joblist);
				for (Job job : joblist) {
					//������ɵ����
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
	//�ҵ���������
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
			//llJobs�е���ҵִ����Ϣ�Ѿ������µ���ҵ��
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
	//�ҵ��������TC��С����
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
	//�޷��ҵ�ES������
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
		//SES��ʼ״̬Ϊֻ����J0���б���
		SES.add(joblist.get(0));
		for (int j= 1; j < joblist.size(); j++) {
			Job job = joblist.get(j);
			List<Job> list = SES;
			//������б���
			List<List<Job>> listsForCheck = new ArrayList<List<Job>>();
			for(int k = 0; k <= list.size(); k++){
				List<Job> ltJobs = new ArrayList<Job>(list);
				ltJobs.add(k, job);
				listsForCheck.add(ltJobs);
			}
			List<List<Job>> listsForCheckBk = new ArrayList<List<Job>>();
			//���б��е�ÿһ������ִ��һ��
			for (List<Job> list2 : listsForCheck) {
				Tools.clearJobInfo(list2);
				Tools.clearJobTaskList(list2);
				Tools.clearResources(schedule);
				List<Job> tList = deepCopy(runlist(schedule, list2));
				listsForCheckBk.add(tList);
			}
			
			//�����ܳͷ����۴�С��������
			Collections.sort(listsForCheckBk, new ListComparator());
			//��ӡ�ܳͷ�����
			for (List<Job> list2 : listsForCheckBk) {
				printAlist(list2);
				System.out.print(":");
				System.out.println(getTotalPenaltyCost(list2));
			}
			SES = listsForCheckBk.get(0);
		}
		return SES;
	}
	
	//�ҵ�TC��С�� 
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
	//���㵱ǰ��ҵ˳���Total TimeCost
	public long getTotalPenaltyCost(List<Job> joblist){
		long totalPC = 0;
		for (Job job : joblist) {
			totalPC += ((job.getFinishTime() - job.getDeadline()) > 0 ? (job.getFinishTime() - job.getDeadline()) : 0);
		}
		return totalPC;
	}

	//��ӡArrayList
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