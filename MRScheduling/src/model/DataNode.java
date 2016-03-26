package model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import model.Slot;
//slot
public class DataNode implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5333413585806372947L;
	private int nodeID;
	private List<Slot> mapSlots = new ArrayList<Slot>();
	private List<Slot> reduceSlots = new ArrayList<Slot>();
	
	public List<Slot> getMapSlots() {
		return mapSlots;
	}
	public void setMapSlots(List<Slot> mapSlots) {
		this.mapSlots = mapSlots;
	}
	public List<Slot> getReduceSlots() {
		return reduceSlots;
	}
	public void setReduceSlots(List<Slot> reduceSlots) {
		this.reduceSlots = reduceSlots;
	}
	private long curFinishTime;
	private double sortKey;
	
	public DataNode(int id)
	{
		this.nodeID = id;
	}
	//slot type
	public enum Stype{Map, Reduce}
	public int getNodeID() {
		return nodeID;
	}
	public void setNodeID(int nodeID) {
		this.nodeID = nodeID;
	}
	
	public long getCurFinishTime() {
		return curFinishTime;
	}
	public void setCurFinishTime(long curFinishTime) {
		this.curFinishTime = curFinishTime;
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
