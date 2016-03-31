package model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Task{

	/**
	 * 
	 */

	private int taskID;
	private int jobID;
	
	private double processTime;
	private double setupTime;
	private double finishTime;
	private double startTime;
	
	private double inputSize;
	public boolean isProcessed() {
		return processed;
	}
	public void setProcessed(boolean processed) {
		this.processed = processed;
	}
	private double outputSize;
	private boolean processed;
	private Job job;
	
	private double sortKey;
	//data size on each node
	//private int[] dataLocations;
	//<node id, data size>
	private HashMap<Integer, Integer> dataLocations = new HashMap<Integer, Integer>();
	private int reduceDataNode;
	public int getReduceDataNode() {
		return reduceDataNode;
	}
	public void setReduceDataNode(int reduceDataNode) {
		this.reduceDataNode = reduceDataNode;
	}
	//host list for task data
	private ArrayList<Integer> hostList = new ArrayList<Integer>();
	
	private TaskType type;
	
	public Task(int tid, double pTime, TaskType type, Job job)
	{
		this.taskID = tid;
		//this.jobID = jid;
		this.processTime = pTime;
		this.type = type;
		this.setupTime = 0;
		this.job = job;
	}
	public int getTaskID() {
		return taskID;
	}
	public void setTaskID(int taskID) {
		this.taskID = taskID;
	}
	public int getJobID() {
		return jobID;
	}
	public void setJobID(int jobID) {
		this.jobID = jobID;
	}
	public double getProcessTime() {
		return processTime;
	}
	public void setProcessTime(double processTime) {
		this.processTime = processTime;
	}
	public double getSetupTime() {
		return setupTime;
	}
	public void setSetupTime(double setupTime) {
		this.setupTime = setupTime;
	}
	public double getInputSize() {
		return inputSize;
	}
	public void setInputSize(double inputSize) {
		this.inputSize = inputSize;
	}
	public double getOutputSize() {
		return outputSize;
	}
	public void setOutputSize(double outputSize) {
		this.outputSize = outputSize;
	}
	public TaskType getType() {
		return type;
	}
	public void setType(TaskType type) {
		this.type = type;
	}
	public HashMap<Integer, Integer> getDataLocations() {
		return dataLocations;
	}
	public void setDataLocations(HashMap<Integer, Integer> dataLocations) {
		this.dataLocations = dataLocations;
	}
	public Job getJob() {
		return job;
	}
	public void setJob(Job job) {
		this.job = job;
	}
	public double getFinishTime() {
		return finishTime;
	}
	public void setFinishTime(double finishTime) {
		this.finishTime = finishTime;
	}
	public void setHostList(ArrayList<Integer> hostList) {
		this.hostList = hostList;
	}
	public ArrayList<Integer> getHostList() {
		return hostList;
	}
	public void setSortKey(double sortKey) {
		this.sortKey = sortKey;
	}
	public double getSortKey() {
		return sortKey;
	}
	public void setStartTime(double startTime) {
		this.startTime = startTime;
	}
	public double getStartTime() {
		return startTime;
	}
	/*public ArrayList<Integer> getHostList() {
		return hostList;
	}
	public void setHostList(ArrayList<Integer> hostList) {
		this.hostList = hostList;
	}*/
	public enum TaskType {MAP, REDUCE}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
