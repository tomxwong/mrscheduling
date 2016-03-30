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

	public static void clearResources(Schedule s){
		for (DataNode dn : s.getCluster().getMapNodes()) {
			for (Slot it_slot : dn.getMapSlots()) {
				it_slot.setCurFinishTime(0);
				it_slot.setStartTime(0);
			}
		}
		for (DataNode dn : s.getCluster().getReduceNodes()) {
			for (Slot it_slot : dn.getReduceSlots()) {
				it_slot.setCurFinishTime(0);
				it_slot.setStartTime(0);
			}
		}
	}
	public static void clearJobInfo(List<Job> joblist){
		for(Job job : joblist)
		{
			//MAP�׶����ʱ��
			job.setMapFinishTime(0);
			//REDUCE�׶����ʱ��
			job.setReduceFinishTime(0);
			//��ҵ��ʼʱ��
			job.setStartTime(0);
			//��ҵ���ʱ��
			job.setFinishTime(0);
			//�ͷ�����
			job.setPenaltyCost(0);
			//�������ؼ���
			job.setSortKey(0);
			//����������Ϣ
			for (Task task : job.getMaps()) {
				task.setFinishTime(0);
				task.setOutputSize(0);
				task.setProcessed(false);
				task.setReduceDataNode(0);
				task.setSortKey(0);
				task.setStartTime(0);
			}
			for (Task task : job.getReduces()) {
				task.setFinishTime(0);
				task.setOutputSize(0);
				task.setProcessed(false);
				task.setSortKey(0);
				task.setStartTime(0);
			}
		}
	}
}
