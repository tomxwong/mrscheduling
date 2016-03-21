package model;

import java.util.ArrayList;
import java.util.List;
import model.Slot;
//slot
public class DataNode {

	private int nodeID;
	private Stype type;
	
	//the current longest and shortest task assigned to the slot
	//private Task longest;
	//private Task shortest;
	private List<Task> tasks = new ArrayList<Task>();
	private List<Slot> slots = new ArrayList<Slot>();
	public List<Slot> getSlots() {
		return slots;
	}
	public void setSlots(ArrayList<Slot> slots) {
		this.slots = slots;
	}
	private long curFinishTime;
	private double sortKey;
	
	public DataNode(int id, Stype type)
	{
		this.nodeID = id;
		this.type = type;
	}
	//slot type
	public enum Stype{Map, Reduce}
	public int getNodeID() {
		return nodeID;
	}
	public void setNodeID(int nodeID) {
		this.nodeID = nodeID;
	}
	public Stype getType() {
		return type;
	}
	public void setType(Stype type) {
		this.type = type;
	}
	public long getCurFinishTime() {
		return curFinishTime;
	}
	public void setCurFinishTime(long curFinishTime) {
		this.curFinishTime = curFinishTime;
	}
	public void setTasks(ArrayList<Task> tasks) {
		this.tasks = tasks;
	}
	public List<Task> getTasks() {
		return tasks;
	}
	public void setSortKey(double sortKey) {
		this.sortKey = sortKey;
	}
	public double getSortKey() {
		return sortKey;
	}
	/*public Task getLongest() {
		return longest;
	}
	public void setLongest(Task longest) {
		this.longest = longest;
	}
	public Task getShortest() {
		return shortest;
	}
	public void setShortest(Task shortest) {
		this.shortest = shortest;
	}*/
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
