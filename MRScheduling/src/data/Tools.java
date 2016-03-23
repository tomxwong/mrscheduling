package data;

import java.util.ArrayList;

import model.DataNode;
import model.Job;
import model.Schedule;
import model.Slot;
import model.Task;

public class Tools {

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
