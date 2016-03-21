
package algorithms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;





import data.Parameters;
import data.RandomInstanceFile;
import data.Tools;
import model.Job;
import model.JobComparator;
import model.Schedule;
import model.Task;


public class MainChunk{
	
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
    		System.out.print("��ҵ����: ");
    		schedule.printAlist(jlist);
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
    		System.out.println("�ܳͷ�����: " + schedule.calculateTotalPenaltyCost(rJlist));
        	///////////////////////////////////
        }
    }

	public void start(Schedule schedule) {
		// TODO Auto-generated method stub
		Tools.clearSchedule(schedule);
    	long currentTime = 0;
		List<Job> jlist = schedule.GetJobSequence(schedule.getJobs());
		List<Job> rJlist = new ArrayList<Job>();
		System.out.print("����ҵ����: ");
		schedule.printAlist(jlist);
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
		//////////////////////////
//		System.out.print("Map�׶���ɺ����ҵ���У�");
//		schedule.printAlist(rJlist);
//		System.out.println("");
//		Job jj = null;
//		for(int i = 0; i < rJlist.size(); i++)
//		{
//		jj = rJlist.get(i);
//		jj.setSortKey(jj.getDeadline());
//		}
//		Collections.sort(rJlist, new JobComparator());
//		System.out.print("Reduce�׶ΰ�EDF�����ĳ�ʼ��ҵ���У�");
//		schedule.printAlist(rJlist);
//		System.out.println("");
		///////////////////////////

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
				//�Ե�ǰ����ҵ�б���urgent����
				Job ji = null;
				for(int hi = 0; hi < rJlist.size(); hi++)
				{
					ji = rJlist.get(hi);
					ji.setSortKey((double)(ji.getDeadline() - currentTime) / ji.getDeadline());
				}
				Collections.sort(rJlist, new JobComparator());
				i = -1;
				break;
			}
		}
		System.out.print(" reduce�׶����ʱ��:" + FinishTime);	
		System.out.println("�������ܳͷ�����: " + schedule.calculateTotalPenaltyCost(rJlist));
    	///////////////////////////////////
		
	}

	public void output() {
		// TODO Auto-generated method stub
		
	}
	public static void main(String[] args){
		Parameters p = new Parameters();
		p.setP_io_rate(0.1);
		p.setP_jobs(4);
//		p.setP_node(20);
		p.setP_rack(2);
		p.setP_node(4);
//		p.setP_map_slot(4);
//		p.setP_reduce_slot(2);
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
		
		//RandomInstance ri = new RandomInstance(p, "1");
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
	private class taskComparator implements Comparator<Object>{
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
}