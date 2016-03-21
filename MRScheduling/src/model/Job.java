package model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Job {
	private int jobID;
	private int mapNum;
	private int reduceNum;
	
	//key for sort
	private double sortKey;
	//private double JRValue;
	//input data size of a job, evenly distributed to each map task
	private int dataSize;
	//only used in some methods
	private long startTime;
	private int priority;
	private long deadline;
	public long getDeadline() {
		return deadline;
	}
	public void setDeadline(long deadline) {
		this.deadline = deadline;
	}
	public static double[] getIo_rate() {
		return io_rate;
	}
	public static void setIo_rate(double[] io_rate) {
		Job.io_rate = io_rate;
	}
	public static int[] getTask_size() {
		return task_size;
	}
	public static void setTask_size(int[] task_size) {
		Job.task_size = task_size;
	}
	private int maxMapMachine;
	private long mapTC;
	private long reduceTC;
	private long penaltyCost;
	public long getPenaltyCost() {
		return penaltyCost;
	}
	public void setPenaltyCost(long penaltyCost) {
		this.penaltyCost = penaltyCost;
	}
	public long getMapTC() {
		return mapTC;
	}
	public void setMapTC(long mapTC) {
		this.mapTC = mapTC;
	}
	public long getReduceTC() {
		return reduceTC;
	}
	public void setReduceTC(long reduceTC) {
		this.reduceTC = reduceTC;
	}
	private long finishTime;
	private long mapFinishTime;
	private long reduceFinishTime;
	public long getReduceFinishTime() {
		return reduceFinishTime;
	}
	public void setReduceFinishTime(long reduceFinishTime) {
		this.reduceFinishTime = reduceFinishTime;
	}
	private ArrayList<Task> maps = new ArrayList<Task>();
	private ArrayList<Task> reduces = new ArrayList<Task>();
	
	//private ArrayList<DataNode> mapTaskSlots = new ArrayList<DataNode>();
	
	private ArrayList<Integer> mapMachines = new ArrayList<Integer>();
	
	private Map<Integer, Integer> slotMapTaskNum = new HashMap<Integer, Integer>();//num of map assigned to the node that the slot resides
	public static double[] io_rate = {0.2,0.4,0.6,0.8,1.0};//{0.1, 0.5, 1.0};
	public static int[] task_size = {128,192,256,320,384};//{64, 128, 192, 256, 320};
	
	public Job(int id, int m, int r)
	{
		this.jobID = id;
		this.mapNum = m;
		this.reduceNum = r;
	}
	public int getJobID() {
		return jobID;
	}
	public void setJobID(int jobID) {
		this.jobID = jobID;
	}
	public int getMapNum() {
		return mapNum;
	}
	public void setMapNum(int mapNum) {
		this.mapNum = mapNum;
	}
	public int getReduceNum() {
		return reduceNum;
	}
	public void setReduceNum(int reduceNum) {
		this.reduceNum = reduceNum;
	}
	public long getStartTime() {
		return startTime;
	}
	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}
	public int getPriority() {
		return priority;
	}
	public void setPriority(int priority) {
		this.priority = priority;
	}
	public int getDataSize() {
		return dataSize;
	}
	public void setDataSize(int dataSize) {
		this.dataSize = dataSize;
	}
	public double getSortKey() {
		return sortKey;
	}
	public void setSortKey(double sortKey) {
		this.sortKey = sortKey;
	}
	public ArrayList<Task> getMaps() {
		return maps;
	}
	public void setMaps(ArrayList<Task> maps) {
		this.maps = maps;
	}
	public ArrayList<Task> getReduces() {
		return reduces;
	}
	public void setReduces(ArrayList<Task> reduces) {
		this.reduces = reduces;
	}
	public long getFinishTime() {
		return finishTime;
	}
	public void setFinishTime(long finishTime) {
		this.finishTime = finishTime;
	}
	public long getMapFinishTime() {
		return mapFinishTime;
	}
	public void setMapFinishTime(long mapFinishTime) {
		this.mapFinishTime = mapFinishTime;
	}
	/*public ArrayList<DataNode> getMapTaskSlots() {
		return mapTaskSlots;
	}*/
	public void setSlotMapTaskNum(Map<Integer, Integer> slotMapTaskNum) {
		this.slotMapTaskNum = slotMapTaskNum;
	}
	public Map<Integer, Integer> getSlotMapTaskNum() {
		return slotMapTaskNum;
	}
	public void setMapMachines(ArrayList<Integer> mapMachines) {
		this.mapMachines = mapMachines;
	}
	public ArrayList<Integer> getMapMachines() {
		return mapMachines;
	}
	public void setMaxMapMachine(int maxMapMachine) {
		this.maxMapMachine = maxMapMachine;
	}
	public int getMaxMapMachine() {
		return maxMapMachine;
	}
	
	
	
	
	
}