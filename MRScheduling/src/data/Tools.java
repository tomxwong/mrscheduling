package data;

import java.util.List;
import java.util.ArrayList;

import model.Cluster;
import model.DataNode;
import model.Job;
import model.Schedule;
import model.Slot;
import model.Task;

public class Tools {
	public static void clearJobList(List<Job> joblist){
		for (Job job : joblist) {
			for (Task task : job.getMaps()) {
				task.setFinishTime(0);
				task.setOutputSize(0);
				task.setProcessed(false);
				task.setReduceDataNode(-1);
				task.setSortKey(0);
				task.setStartTime(0);
			}
		}
	}
	public static void clearDataNode(Cluster cluster){
		for (DataNode dn : cluster.getReduceNodes()) {
			for (Slot it_slot : dn.getSlots()) {
				it_slot.setCurFinishTime(0);
				it_slot.setStartTime(0);
			}
		}
	}
	public static void clearSchedule(Schedule s) {
		//������ҵ�������Ϣ
		for(Job job : s.getJobs())
		{
			//MAP�׶����ʱ��
			job.setMapFinishTime(0);
			//REDUCE�׶����ʱ��
			job.setReduceFinishTime(0);
			//��ҵ��ʼʱ��
			job.setStartTime(0);
			//��ҵ���ʱ��
			job.setFinishTime(0);
			//�����
			job.setSortKey(0);
			//�ͷ�����
			job.setPenaltyCost(0);
			
			//����MAP����������Ϣ
			for(Task map : job.getMaps())
			{
				//��ʼʱ��
				map.setStartTime(0);
				//����ʱ��
				map.setFinishTime(0);
				//�����
				map.setSortKey(0);
				//�Ƿ����
				map.setProcessed(false);
			}
			//����REDUCE����������Ϣi
			for(Task reduce : job.getReduces())
			{
				//��ʼʱ��
				reduce.setStartTime(0);
				//����ʱ��
				reduce.setFinishTime(0);
				//�����
				reduce.setSortKey(0);
				//�Ƿ����
				reduce.setProcessed(false);
			}
		}
		
		//����Ⱥ��MAP���ݽڵ������Ϣ
		for(DataNode mn : s.getCluster().getMapNodes())
		{
			//��ʼʱ��
			mn.setCurFinishTime(0);
			//�����
			mn.setSortKey(0);
			//��������б�
			mn.setTasks(new ArrayList<Task>());
			//�ڵ��ϵ�SLOT��Ϣ
			for (Slot slot : mn.getSlots()) {
				//SLOT��ʼʱ��
				slot.setStartTime(0);
				//SLOT��ǰ��������ʱ��
				slot.setCurFinishTime(0);
			}
		}
		
		//����ȺREDUCE���ݽڵ������Ϣ
		for(DataNode rn : s.getCluster().getReduceNodes())
		{
			//��ʼʱ��
			rn.setCurFinishTime(0);
			//�����
			rn.setSortKey(0);
			//��������б�
			rn.setTasks(new ArrayList<Task>());
			//�ڵ��ϵ�SLOT��Ϣ
			for (Slot slot : rn.getSlots()) {
				//SLOT��ʼʱ��
				slot.setStartTime(0);
				//SLOT��ǰ��������ʱ��
				slot.setCurFinishTime(0);
			}
		}
		
	}

}
