package model;

import java.io.Serializable;

public class Slot implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1647427867940227748L;
	private int slotID;
	private int nodeID;
	SlotType type;
	public int getNodeID() {
		return nodeID;
	}

	public void setNodeID(int nodeID) {
		this.nodeID = nodeID;
	}

	private long startTime = 0;
	private long curFinishTime = 0;
	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getCurFinishTime() {
		return curFinishTime;
	}

	public void setCurFinishTime(long curFinishTime) {
		this.curFinishTime = curFinishTime;
	}

	public Slot(int slotID, SlotType type) {
		super();
		this.slotID = slotID;
		this.type = type;
	}

	public SlotType getType() {
		return type;
	}

	public void setType(SlotType type) {
		this.type = type;
	}

	public int getSlotID() {
		return slotID;
	}

	public void setSlotID(int slotID) {
		this.slotID = slotID;
	}
	public enum SlotType {MAP, REDUCE}
}