package model;

import java.io.Serializable;

public class Slot{

	private int slotID;
	private int nodeID;
	SlotType type;
	public int getNodeID() {
		return nodeID;
	}

	public void setNodeID(int nodeID) {
		this.nodeID = nodeID;
	}

	private double startTime = 0;
	private double curFinishTime = 0;
	public double getStartTime() {
		return startTime;
	}

	public void setStartTime(double startTime) {
		this.startTime = startTime;
	}

	public double getCurFinishTime() {
		return curFinishTime;
	}

	public void setCurFinishTime(double curFinishTime) {
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