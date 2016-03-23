package model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import data.Tools;
import model.Task.TaskType;

//import org.apache.commons.collections.BinaryHeap;

public class Schedule {

	private List<Task> maps = new ArrayList<Task>();
	private List<Task> reduces = new ArrayList<Task>();
	private List<Job> jobs = new ArrayList<Job>();
	
	
	private Cluster cluster;
	private double ioRate = 0;
	
	//record the current longest finishing time of all the slots
	private long firstAvail = 0;
	
	private DataNode firstSlot = null;
	
	//record the current finishing time of each slot in order
	private ArrayList<DataNode> curAvailList = new ArrayList<DataNode>();
	//private BinaryHeap earlestSlot = new BinaryHeap(true, new SlotComparator());
	//the latest map task time of each job
	private Map<Integer, Long> jobMaxMapTime = new HashMap<Integer, Long>();
	
	
	
	
	//the assignment of tasks to slot
	//integer:nodeID,list:tasks
	private Map<Integer, ArrayList<Task>> mapAssignment = new HashMap<Integer, ArrayList<Task>>();
	private Map<Integer, ArrayList<Task>> reduceAssignment = new HashMap<Integer, ArrayList<Task>>();
	
	private Map<Integer, Integer> mapTask2Slot = new HashMap<Integer, Integer>();
	private Map<Integer, Integer> reduceTask2Slot = new HashMap<Integer, Integer>();
		
	private long makespan = 0;
	
	public Schedule(Schedule s)
	{
		this.cluster = s.getCluster();
		this.jobs = s.getJobs();
		this.maps = s.getMaps();
		this.reduces = s.getReduces();
		
		this.makespan = 0;
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
								task.setOutputSize(task.getInputSize() * task.getJob().getIo_rate());
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
							//任务结束时间
							task.setFinishTime(finishTime);
							//当前MAP任务输出数据大小
							task.setOutputSize(task.getInputSize() * this.getIoRate());
							//记录MAP结果输出数据节点
							task.setReduceDataNode(nodeId);
							//当前DATANODE的结束时间
							sl.setCurFinishTime(sl.getCurFinishTime() + tepSetupTime + task.getProcessTime());
							//任务设置为已处理
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


	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}
}
