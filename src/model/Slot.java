package model;
public class Slot{
	private int slotID;
	private int nodeID;
	public int getNodeID() {
		return nodeID;
	}

	public void setNodeID(int nodeID) {
		this.nodeID = nodeID;
	}

	private long startTime;
	private long curFinishTime;
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

	public Slot(int slotID) {
		super();
		this.slotID = slotID;
	}

	public int getSlotID() {
		return slotID;
	}

	public void setSlotID(int slotID) {
		this.slotID = slotID;
	}
}