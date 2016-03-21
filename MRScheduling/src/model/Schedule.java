package model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

//import com.sun.jndi.url.dns.dnsURLContext;
//import com.sun.org.apache.xalan.internal.xsltc.runtime.Parameter;

import data.Parameters;
import data.Tools;
import model.Task.TaskType;

//import org.apache.commons.collections.BinaryHeap;

public class Schedule {

	private List<Task> maps = new ArrayList<Task>();
	private List<Task> reduces = new ArrayList<Task>();
	private List<Job> jobs = new ArrayList<Job>();
	
	
	private Cluster cluster;
	private double ioRate = 0;
	/*private long[] cMapTime;
	private long[] cReduceTime;*/
	
	//record the current longest finishing time of all the slots
	private long firstAvail = 0;
	
	private DataNode firstSlot = null;
	
	//record the current finishing time of each slot in order
	private ArrayList<DataNode> curAvailList = new ArrayList<DataNode>();
	//private BinaryHeap earlestSlot = new BinaryHeap(true, new SlotComparator());
	//the latest map task time of each job
	private Map<Integer, Long> jobMaxMapTime = new HashMap<Integer, Long>();
	
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
		@Override
		public int compare(Object o1, Object o2) {
			// TODO Auto-generated method stub
			List<Job> list1 = (List<Job>)o1;
			List<Job> list2 = (List<Job>)o2;
			long r1 = estimateTotalPenaltyCost(list1),r2 = estimateTotalPenaltyCost(list2);
			if(r1 > r2){
				return 1;
			}else if(r1 == r2){
				return 0;
			}else{
				return -1;
			}
		}
	}
	//the assignment of tasks to slot
	//integer:nodeID,list:tasks
	private Map<Integer, ArrayList<Task>> mapAssignment = new HashMap<Integer, ArrayList<Task>>();
	private Map<Integer, ArrayList<Task>> reduceAssignment = new HashMap<Integer, ArrayList<Task>>();
	
	private Map<Integer, Integer> mapTask2Slot = new HashMap<Integer, Integer>();
	private Map<Integer, Integer> reduceTask2Slot = new HashMap<Integer, Integer>();
		
	private long makespan = 0;
	private long lowerBound = 0;
	
	public Schedule(Schedule s)
	{
		this.cluster = s.getCluster();
		this.jobs = s.getJobs();
		this.maps = s.getMaps();
		this.reduces = s.getReduces();
		
		this.makespan = 0;
		this.lowerBound = 0;
	}
	public Schedule(Cluster cluster)
	{
		this.cluster = cluster;
		this.firstAvail = 0;
	}

	
	//return the setup time when t is assigned to n
	public int calculateSetupTime(Task t, DataNode n)
	{
		if(t.getType() == TaskType.MAP)
		{
			if(t.getDataLocations().get(n.getNodeID()) > 0){
				t.setSetupTime(t.getDataLocations().get(n.getNodeID()) / Cluster.LOCAL_RATE);
				return 1;
			}
			else{
				for(int i : t.getHostList()){
					if(cluster.getTopo()[i][n.getNodeID()] == 2){
						t.setSetupTime((long)t.getInputSize() / Cluster.RACK_RATE);
						return 2;
					}
				}
				t.setSetupTime((long)t.getInputSize() / Cluster.REMOTE_RATE);//note:distinguish rack and remote
				return 3;
			}
		}
		else
		{
			double unitSize = (t.getInputSize() / t.getJob().getMapNum());//t.getInputSize() / t.getJob().getMapNum();
			double rLocal = 0;
			double rRack = 0;
			double rRemote = 0;
			for(Task map : t.getJob().getMaps())
			{
				if(cluster.getTopo()[mapTask2Slot.get(map.getTaskID())][n.getNodeID()] == 1)
					rLocal += unitSize / Cluster.LOCAL_RATE;
				else if(cluster.getTopo()[mapTask2Slot.get(map.getTaskID())][n.getNodeID()] == 2)
					rRack += unitSize / Cluster.RACK_RATE;
				else
					rRemote += unitSize / Cluster.REMOTE_RATE;
			}
			t.setSetupTime((long)(rLocal + rRack + rRemote));
			if(getMax(rLocal, rRack, rRemote) == 1)
				return 1;
			else if(getMax(rLocal, rRack, rRemote) == 2) return 2;
			else
				return 3;
		}
		//return 0;
	}
	private int getMax(double rLocal, double rRack, double rRemote) {
		double max1 = Math.max(rLocal, rRack);
		double max2 = Math.max(max1, rRemote);
		if(max2 == rRemote)return 3;
		else if(rLocal < rRack) return 2;
		else return 1;
	}
	public long calculateMakespan()
	{
		//calculateSetupTime();
		
		/*s.setcMapTime(new long[s.getMaps().size()]);
		s.setcReduceTime(new long[s.getReduces().size()]);*/
		
		long[] curMapLast = new long[cluster.getMapNodes().size()];
		long[] curReduceLast = new long[cluster.getReduceNodes().size()];
		
		//long maxMap = Long.MIN_VALUE;
		for(int key : mapAssignment.keySet())
		{
			for(Task t : mapAssignment.get(key))
			{
				t.setFinishTime(curMapLast[key - 1] + t.getSetupTime() + t.getProcessTime());
				curMapLast[key - 1] = t.getFinishTime();
				if(t.getFinishTime() > jobMaxMapTime.get(t.getJob().getJobID()))
					jobMaxMapTime.put(t.getJob().getJobID(), t.getFinishTime());
			}
			/*if(curMapLast[key - 1] > maxMap)
				maxMap = curMapLast[key - 1];*/
		}
		
		long maxReduce = Long.MIN_VALUE;
		int mapSlots = cluster.getMapNodes().size();
		for(int key : reduceAssignment.keySet())
		{
			for(Task t : reduceAssignment.get(key))
			{
				if(jobMaxMapTime.get(t.getJob().getJobID()) > curReduceLast[key - mapSlots - 1])
					t.setFinishTime(jobMaxMapTime.get(t.getJob().getJobID()) + t.getSetupTime() + t.getProcessTime());
				else
					t.setFinishTime(curMapLast[key - 1] + t.getSetupTime() + t.getProcessTime());
				curReduceLast[key - mapSlots - 1] = t.getFinishTime();
			}
			if(curReduceLast[key - mapSlots - 1] > maxReduce)
				maxReduce = curReduceLast[key - mapSlots - 1];
		}
		return maxReduce;
	}
	/*private void calculateSetupTime() {
		for(int key : mapAssignment.keySet())
		{
			for(Task t : mapAssignment.get(key))
			{
				if(t.getDataLocations().get(key) > 0)
					t.setSetupTime(t.getDataLocations().get(key) / Cluster.LOCAL_RATE);
				else
					t.setSetupTime(t.getInputSize() / Cluster.RACK_RATE);//note:distinguish rack and remote
			}
		}
		
		for(int key : reduceAssignment.keySet())
		{
			for(Task t : reduceAssignment.get(key))
			{
				int unitSize = t.getInputSize() / t.getJob().getMapNum();
				long rLocal = 0;
				long rRack = 0;
				long rRemote = 0;
				for(Task map : t.getJob().getMaps())
				{
					if(cluster.getTopo()[mapTask2Slot.get(map.getTaskID())][key] == 1)
						rLocal += unitSize / Cluster.LOCAL_RATE;
					else if(cluster.getTopo()[mapTask2Slot.get(map.getTaskID())][key] == 2)
						rRack += unitSize / Cluster.RACK_RATE;
					else
						rRemote += unitSize / Cluster.REMOTE_RATE;
				}
				t.setSetupTime(rLocal + rRack + rRemote);
			}
		}
		
	}*/
	public List<Task> getMaps() {
		return maps;
	}

	public void setMaps(List<Task> maps) {
		this.maps = maps;
	}

	public List<Task> getReduces() {
		return reduces;
	}

	public void setReduces(List<Task> reduces) {
		this.reduces = reduces;
	}

	public List<Job> getJobs() {
		return jobs;
	}

	public void setJobs(List<Job> jobs) {
		this.jobs = jobs;
	}

	public long getMakespan() {
		return makespan;
	}

	public void setMakespan(long makespan) {
		this.makespan = makespan;
	}
	public Map<Integer, ArrayList<Task>> getMapAssignment() {
		return mapAssignment;
	}
	public void setMapAssignment(Map<Integer, ArrayList<Task>> mapAssignment) {
		this.mapAssignment = mapAssignment;
	}
	public Map<Integer, ArrayList<Task>> getReduceAssignment() {
		return reduceAssignment;
	}
	public void setReduceAssignment(Map<Integer, ArrayList<Task>> reduceAssignment) {
		this.reduceAssignment = reduceAssignment;
	}
	public Map<Integer, Long> getJobMaxMapTime() {
		return jobMaxMapTime;
	}
	public void setJobMaxMapTime(Map<Integer, Long> jobMaxMapTime) {
		this.jobMaxMapTime = jobMaxMapTime;
	}
	public Map<Integer, Integer> getMapTask2Slot() {
		return mapTask2Slot;
	}
	public void setMapTask2Slot(Map<Integer, Integer> mapTask2Slot) {
		this.mapTask2Slot = mapTask2Slot;
	}
	public Map<Integer, Integer> getReduceTask2Slot() {
		return reduceTask2Slot;
	}
	public void setReduceTask2Slot(Map<Integer, Integer> reduceTask2Slot) {
		this.reduceTask2Slot = reduceTask2Slot;
	}
	public Cluster getCluster() {
		return cluster;
	}
	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}
	public long getFirstAvail() {
		return firstAvail;
	}
	public void setFirstAvail(long firstAvail) {
		this.firstAvail = firstAvail;
	}
	public DataNode getFirstSlot() {
		return firstSlot;
	}
	public void setFirstSlot(DataNode firstSlot) {
		this.firstSlot = firstSlot;
	}
	public ArrayList<DataNode> getCurAvailList() {
		return curAvailList;
	}
	public void setCurAvailList(ArrayList<DataNode> curAvailList) {
		this.curAvailList = curAvailList;
	}
	public double getIoRate() {
		return ioRate;
	}
	public void setIoRate(double ioRate) {
		this.ioRate = ioRate;
	}
	/*public long[] getcReduceTime() {
		return cReduceTime;
	}
	public void setcReduceTime(long[] cReduceTime) {
		this.cReduceTime = cReduceTime;
	}
	public long[] getcMapTime() {
		return cMapTime;
	}
	public void setcMapTime(long[] cMapTime) {
		this.cMapTime = cMapTime;
	}*/
	
	//为当前任务找到一个最小准备时间的节点执行
	public long assignTaskNode(Task task,long currentTime){
		TaskType taskType = task.getType();
		byte[][] topo = this.cluster.getTopo();
		if(taskType == TaskType.MAP){
			
			for (int nodeId : task.getHostList()) {
				for(DataNode nd : this.cluster.getMapNodes()){
					if(topo[nodeId][nd.getNodeID()] == 1){
						for (Slot sl : nd.getSlots()) {
							if(sl.getCurFinishTime() <= currentTime){
								long tepSetupTime = (long)task.getInputSize() / Cluster.REMOTE_RATE;
								long finishTime = currentTime + tepSetupTime + task.getProcessTime();
								task.setFinishTime(finishTime);
								task.setOutputSize(task.getInputSize() * this.getIoRate());
								task.setReduceDataNode(nodeId);
								sl.setCurFinishTime(sl.getCurFinishTime() + tepSetupTime + task.getProcessTime());
								task.setProcessed(true);
								return tepSetupTime + task.getProcessTime();
							}
						}
					}					
				}
			}
		//rack level process
		for (int nodeId : task.getHostList()) {
			for(DataNode nd : this.cluster.getMapNodes()){
				if(topo[nodeId][nd.getNodeID()] == 2){
					for (Slot sl : nd.getSlots()) {
						if(sl.getCurFinishTime() <= currentTime){
							long tepSetupTime = (long)task.getInputSize() / Cluster.RACK_RATE;
							long finishTime = currentTime + tepSetupTime + task.getProcessTime();
							task.setFinishTime(finishTime);
							task.setOutputSize(task.getInputSize() * this.getIoRate());
							task.setReduceDataNode(nodeId);
							sl.setCurFinishTime(sl.getCurFinishTime() + tepSetupTime + task.getProcessTime());
							task.setProcessed(true);
							return tepSetupTime + task.getProcessTime();
						}
					}
				}					
			}
		}
		// remote level process
		for (int nodeId : task.getHostList()) {
			for(DataNode nd : this.cluster.getMapNodes()){
				if(topo[nodeId][nd.getNodeID()] == 3){
					for (Slot sl : nd.getSlots()) {
						if(sl.getCurFinishTime() <= currentTime){
							long tepSetupTime = (long)task.getInputSize() / Cluster.REMOTE_RATE;
							long finishTime = currentTime + tepSetupTime + task.getProcessTime();
							task.setFinishTime(finishTime);
							task.setOutputSize(task.getInputSize() * this.getIoRate());
							task.setReduceDataNode(nodeId);
							sl.setCurFinishTime(sl.getCurFinishTime() + tepSetupTime + task.getProcessTime());
							task.setProcessed(true);
							return tepSetupTime + task.getProcessTime();
						}
					}
				}					
			}
		}
	}else{//reduce 任务
			int maxDataNode = 1	;
			for (DataNode dn : cluster.getReduceNodes()) {
				for (Task maptask : task.getJob().getMaps()) {
					if(topo[dn.getNodeID()][maptask.getReduceDataNode()] == 1){
						for(Slot sl : dn.getSlots()){
							if(sl.getCurFinishTime() <= currentTime){
								maxDataNode = dn.getNodeID();
								long transferTime = 0;
								for(Task eachMapTask : task.getJob().getMaps()){
									if(topo[maxDataNode][eachMapTask.getReduceDataNode()] == 1){
										transferTime += (long)(eachMapTask.getOutputSize() / (task.getJob().getReduces().size() * Cluster.LOCAL_RATE));
									}else if(topo[maxDataNode][eachMapTask.getReduceDataNode()] == 2){
										transferTime += (long)(eachMapTask.getOutputSize() / (task.getJob().getReduces().size() * Cluster.RACK_RATE));
									}else{
										transferTime += (long)(eachMapTask.getOutputSize() / (task.getJob().getReduces().size() * Cluster.REMOTE_RATE));
									}	
								}
								long finishTime = currentTime + transferTime + task.getProcessTime();
								task.setFinishTime(finishTime);
								sl.setCurFinishTime(sl.getCurFinishTime() + transferTime + task.getProcessTime());
								task.setProcessed(true);
								return transferTime + task.getProcessTime();
							}
						}
					}
				}
			}
				for (DataNode dn : cluster.getReduceNodes()) {
					for (Task maptask : task.getJob().getMaps()) {
						if(topo[dn.getNodeID()][maptask.getReduceDataNode()] == 2){
							for(Slot sl : dn.getSlots()){
								if(sl.getCurFinishTime() <= currentTime){
									maxDataNode = dn.getNodeID();
									long transferTime = 0;
									for(Task eachMapTask : task.getJob().getMaps()){
										if(topo[maxDataNode][eachMapTask.getReduceDataNode()] == 1){
											transferTime += (long)(eachMapTask.getOutputSize() / (task.getJob().getReduces().size() * Cluster.LOCAL_RATE));
										}else if(topo[maxDataNode][eachMapTask.getReduceDataNode()] == 2){
											transferTime += (long)(eachMapTask.getOutputSize() / (task.getJob().getReduces().size() * Cluster.RACK_RATE));
										}else{
											transferTime += (long)(eachMapTask.getOutputSize() / (task.getJob().getReduces().size() * Cluster.REMOTE_RATE));
										}	
									}
									long finishTime = currentTime + transferTime + task.getProcessTime();
									task.setFinishTime(finishTime);
									sl.setCurFinishTime(sl.getCurFinishTime() + transferTime + task.getProcessTime());
									task.setProcessed(true);
									return transferTime + task.getProcessTime();
								}
							}
						}
					}
				}
				
				for (DataNode dn : cluster.getReduceNodes()) {
					for (Task maptask : task.getJob().getMaps()) {
						if(topo[dn.getNodeID()][maptask.getReduceDataNode()] == 3){
							for(Slot sl : dn.getSlots()){
								if(sl.getCurFinishTime() <= currentTime){
									maxDataNode = dn.getNodeID();
									long transferTime = 0;
									for(Task eachMapTask : task.getJob().getMaps()){
										if(topo[maxDataNode][eachMapTask.getReduceDataNode()] == 1){
											transferTime += (long)(eachMapTask.getOutputSize() / (task.getJob().getReduces().size() * Cluster.LOCAL_RATE));
										}else if(topo[maxDataNode][eachMapTask.getReduceDataNode()] == 2){
											transferTime += (long)(eachMapTask.getOutputSize() / (task.getJob().getReduces().size() * Cluster.RACK_RATE));
										}else{
											transferTime += (long)(eachMapTask.getOutputSize() / (task.getJob().getReduces().size() * Cluster.REMOTE_RATE));
										}	
									}
									long finishTime = currentTime + transferTime + task.getProcessTime();
									task.setFinishTime(finishTime);
									sl.setCurFinishTime(sl.getCurFinishTime() + transferTime + task.getProcessTime());
									task.setProcessed(true);
									return transferTime + task.getProcessTime();
								}
							}
						}
					}
				}								
		}
		return -1;
	}
	//估算maxspan
	public long estimateMaxspan(List<Job> joblist){
		long mFinishTime = 0;
		long rFinishTime = 0;
		for (Job job : joblist) {
			job.setMapFinishTime(mFinishTime + calculateJobMapTimeCost(job));
			mFinishTime += calculateJobMapTimeCost(job);
			job.setReduceFinishTime(Math.max(rFinishTime,job.getMapFinishTime()) + calculateJobReduceTimeCost(job));
			rFinishTime += calculateJobReduceTimeCost(job);
			job.setFinishTime(job.getReduceFinishTime());
		}
		return joblist.get(joblist.size() - 1).getFinishTime();
	}
	//估计作业的map时间
	public long calculateJobMapTimeCost(Job job){
		double time = 0;
		int nOfLocal = (int)(job.getMaps().size() * cluster.getSitaMl());
		int nOfRack = (int)(job.getMaps().size() * cluster.getSitaMr());
		//int nOfRemote = (int)(job.getMaps().size() * (1 - cluster.getSitaMl() - cluster.getSitaMr()));
		int i = 0;
		for (; i < nOfLocal; i++) {
			time += ((job.getMaps().get(i).getInputSize() / Cluster.LOCAL_RATE) + (job.getMaps().get(i).getProcessTime()));
		}
		for (; i < nOfLocal + nOfRack; i++) {
			time += ((job.getMaps().get(i).getInputSize() / Cluster.RACK_RATE) + (job.getMaps().get(i).getProcessTime()));
		}
		for (; i < job.getMaps().size(); i++) {
			time += ((job.getMaps().get(i).getInputSize() / Cluster.REMOTE_RATE) + (job.getMaps().get(i).getProcessTime()));
		}
		//long mTimeCost =(long) (timel * cluster.getSitaMl() + timer * cluster.getSitaMr() + (1 - cluster.getSitaMl() - cluster.getSitaMr()) * timef);
		return (long)time;
	}
	//估计作业的reduce时间
	public long calculateJobReduceTimeCost(Job job){
		double time_d = 0,time_p = 0;
		//获取总待处理数据大小
		for (Task map_task : job.getMaps()) {
			time_d += map_task.getOutputSize();
		}
		//处理每一个reduce任务的处理时间
		for (Task reduce_task : job.getReduces()) {
			time_p += reduce_task.getProcessTime();
		}
		long rTimeCost = (long)(time_d * cluster.getSitaRl() / cluster.LOCAL_RATE + time_d * cluster.getSitaRr() / cluster.RACK_RATE + (1 - cluster.getSitaRl() - cluster.getSitaRr()) * time_d / cluster.REMOTE_RATE);
		return (long)(time_p + rTimeCost);
	}
	//判断当前job序列是否能够全部按时完成
	public boolean canFinishOnTime(List<Job> joblist){
		long mFinishTime = 0;
		long rFinishTime = 0;
		for (Job job : joblist) {
			long jobMapFinishTime = mFinishTime + calculateJobMapTimeCost(job);
			long jobReduceFinishTime = Math.max(rFinishTime,jobMapFinishTime) + calculateJobReduceTimeCost(job);
			if(jobReduceFinishTime > job.getDeadline()){
				return false;
			}
		}
		return true;
	}
	//计算当前作业顺序的每个作业的Map阶段和Reduce阶段的完成时间
	public List<Job> setFinishTimeOfList(List<Job> joblist){
		long mFinishTime = 0;
		long rFinishTime = 0;
		for (Job job : joblist) {
			long mapFinishTime = mFinishTime + calculateJobMapTimeCost(job);
			job.setMapFinishTime(mapFinishTime);
			long reduceFinishTime = Math.max(rFinishTime,job.getMapFinishTime()) + calculateJobReduceTimeCost(job);
			job.setReduceFinishTime(reduceFinishTime);
			job.setFinishTime(job.getReduceFinishTime());
			mFinishTime  = mapFinishTime;
			rFinishTime = reduceFinishTime;
		}
		return joblist;
	}
	//估算当前作业顺序的Total TimeCost
	public long estimateTotalPenaltyCost(List<Job> joblist){
		long mFinishTime = 0;
		long rFinishTime = 0;
		long totalPC = 0;
		for (Job job : joblist) {
			long jobMapFinishTime = mFinishTime + calculateJobMapTimeCost(job);
			mFinishTime = jobMapFinishTime;
			long jobReduceFinishTime = Math.max(rFinishTime,jobMapFinishTime) + calculateJobReduceTimeCost(job);
			rFinishTime = jobReduceFinishTime;
			if(jobReduceFinishTime > job.getDeadline()){
				totalPC += (jobReduceFinishTime - job.getDeadline());
			}else{
				totalPC += 0;
			}
		}
		return totalPC;
	}
	//计算当前作业顺序的Total TimeCost
	public long calculateTotalPenaltyCost(List<Job> joblist){
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
	public int getNList(List<List<Job>> lists){
		long minPenalty = estimateTotalPenaltyCost(lists.get(0));
		int index = 1;
		for (int i = 1; i < lists.size(); i++) {
			long curPenalty = estimateTotalPenaltyCost(lists.get(i));
			if(curPenalty == minPenalty){
				index ++;
			}
		}
		return index;
	}
	//找到maxspan最小的
	public List<Job> getMinMakeSpanList(List<List<Job>> lists){
		int index = -1;
		long minMaxspan = Integer.MAX_VALUE;
		for (List<Job> list : lists) {
			long curMaxSpan = estimateMaxspan(list);
			if(curMaxSpan < minMaxspan){
				minMaxspan = curMaxSpan;
				index = lists.indexOf(list);
			}
		}
		return lists.get(index);
	}
	//找到TC最小的
		public List<Job> getMinTCList(List<List<Job>> lists){
			int index = -1;
			long minTC = Integer.MAX_VALUE;
			for (List<Job> list : lists) {
				long curTC = calculateTotalPenaltyCost(list);
				if(curTC < minTC){
				minTC = curTC;
				index = lists.indexOf(list);
				}
			}
			return lists.get(index);
		}
		//找到能完成的序列
		public List<List<Job>> getCanFinish(List<List<Job>> lists){
			long mFinishTime = 0;
			long rFinishTime = 0;
			int count = 0;
			List<List<Job>> resultLists = new ArrayList<List<Job>>();
			int index = 0;
			boolean flag = true;
			if(lists.isEmpty())
				return null;
			for (List<Job> joblist : lists) {
				for (Job job : joblist) {
					long maptimecost = calculateJobMapTimeCost(job);
					long jobMapFinishTime = mFinishTime + maptimecost;
					mFinishTime = jobMapFinishTime;
					long reducetimecost = calculateJobReduceTimeCost(job);
					long jobReduceFinishTime = Math.max(rFinishTime,job.getMapFinishTime()) + reducetimecost;
					rFinishTime = jobReduceFinishTime;
					if(jobReduceFinishTime > job.getDeadline()){
						flag = false;
					}
				}
				if(flag == true){
					resultLists.add(joblist);
					count++;
					mFinishTime = 0;
					rFinishTime = 0;
				}else{
					flag = true;
					mFinishTime = 0;
					rFinishTime = 0;
				}
			}
			if(count == 0)
				return null;
			return resultLists;
		}
	//找到能完成且TC最小序列
	public List<Job> getCanFinishMinTC(List<List<Job>> lists){
		long mFinishTime = 0;
		long rFinishTime = 0;
		int count = 0;
		List<List<Job>> resultLists = new ArrayList<List<Job>>();
		int index = 0;
		boolean flag = true;
		if(lists.isEmpty())
			return null;
		for (List<Job> joblist : lists) {
			for (Job job : joblist) {
				long jobMapFinishTime = mFinishTime + calculateJobMapTimeCost(job);
				mFinishTime = jobMapFinishTime;
				long jobReduceFinishTime = Math.max(rFinishTime,job.getMapFinishTime()) + calculateJobReduceTimeCost(job);
				rFinishTime = jobReduceFinishTime;
				if(rFinishTime > job.getDeadline()){
					flag = false;
				}
			}
			if(flag == true){
				resultLists.add(joblist);
				count++;
				mFinishTime = 0;
				rFinishTime = 0;
			}else{
				flag = true;
				mFinishTime = 0;
				rFinishTime = 0;
			}
		}
		if(flag == false && count == 0)
			return null;
		return getMinTCList(resultLists);
	}
	//无法找到ES处理方法
	public List<Job> minimizePenaltyCost(List<Job> joblist){
		Tools.clearSchedule(this);
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
						int nMin = getNList(listsForCheck);												
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
	//打印ArrayList
	public void printAlist(List<Job> joblist){
		for (Job job : joblist) {
			System.out.print(job.getJobID() + " ");
		}
	}

	//找到最优序列
	public List<Job> GetJobSequence(List<Job> joblist){
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
			List<List<Job>> llJobs = getCanFinish(tepLists);
			if(llJobs != null){
				listLeftJobsSequences = llJobs;	
			}else{
				flag = false;
				break;
			}			
			tepLists.clear();
		}
		if(listLeftJobsSequences.isEmpty() || flag == false){
			return minimizePenaltyCost(joblist);
		}else{
			return getMinTCList(listLeftJobsSequences);
		}
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
}
