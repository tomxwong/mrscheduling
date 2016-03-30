package model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Job implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -5865848706027532115L;
	private int jobID;
	private int mapNum;
	private int reduceNum;
	
	//key for sort
	private double sortKey;
	//private double JRValue;
	//input data size of a job, evenly distributed to each map task
	private int dataSize;
	//only used in some methods
	private double startTime;
	private int priority;
	private double deadline;
	public double getDeadline() {
		return deadline;
	}
	public void setDeadline(double deadline) {
		this.deadline = deadline;
	}

	//用于生成数据
	public static double[] io_for_generate={0.1,0.3,0.5,0.7,0.9};
	public static double[] getIo_for_generate() {
		return io_for_generate;
	}
	public static void setIo_for_generate(double[] io_for_generate) {
		Job.io_for_generate = io_for_generate;
	}
	public  void setIo_rate(double io_rate) {
		this.io_rate = io_rate;
	}
	public  double getIo_rate() {
		return io_rate;
	}
	public static int[] getTask_size() {
		return task_size;
	}
	public static void setTask_size(int[] task_size) {
		Job.task_size = task_size;
	}
	private int maxMapMachine;
	private double mapTC;
	private double reduceTC;
	private double penaltyCost;
	public double getPenaltyCost() {
		return penaltyCost;
	}
	public void setPenaltyCost(double penaltyCost) {
		this.penaltyCost = penaltyCost;
	}
	public double getMapTC() {
		return mapTC;
	}
	public void setMapTC(double mapTC) {
		this.mapTC = mapTC;
	}
	public double getReduceTC() {
		return reduceTC;
	}
	public void setReduceTC(double reduceTC) {
		this.reduceTC = reduceTC;
	}
	private double finishTime;
	private double mapFinishTime;
	private double reduceFinishTime;
	public double getReduceFinishTime() {
		return reduceFinishTime;
	}
	public void setReduceFinishTime(double reduceFinishTime) {
		this.reduceFinishTime = reduceFinishTime;
	}
	private ArrayList<Task> maps = new ArrayList<Task>();
	private ArrayList<Task> reduces = new ArrayList<Task>();
	
	//private ArrayList<DataNode> mapTaskSlots = new ArrayList<DataNode>();
	
	private ArrayList<Integer> mapMachines = new ArrayList<Integer>();
	
	private Map<Integer, Integer> slotMapTaskNum = new HashMap<Integer, Integer>();//num of map assigned to the node that the slot resides
	public double io_rate;
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
	public double getStartTime() {
		return startTime;
	}
	public void setStartTime(double startTime) {
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
	public double getFinishTime() {
		return finishTime;
	}
	public void setFinishTime(double finishTime) {
		this.finishTime = finishTime;
	}
	public double getMapFinishTime() {
		return mapFinishTime;
	}
	public void setMapFinishTime(double mapFinishTime) {
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