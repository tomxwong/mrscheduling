package model;

import java.util.ArrayList;
import java.util.List;

public class Cluster {
	
	public static final int LOCAL_RATE = 120;//120;//20;
	public static final int RACK_RATE = 80;//80;//8;
	public static final int REMOTE_RATE = 40;//40;//2;//unit is metabyte
	public static final int DUPLICATE = 4;
	public static final int BLOCK_SIZE = 64;//unit is metabyte, assume that the split size equals block size
	
	//¹À¼ÆMap time cost
	private double sitaMl = 0.3;
	private double sitaMr = 0.5;
	
	//¹À¼ÆReduce time cost
	private double sitaRl = 0.3;
	private double sitaRr = 0.3;
	
	public double getSitaMl() {
		return sitaMl;
	}

	public void setSitaMl(double sitaMl) {
		this.sitaMl = sitaMl;
	}

	public double getSitaMr() {
		return sitaMr;
	}

	public void setSitaMr(double sitaMr) {
		this.sitaMr = sitaMr;
	}

	public double getSitaRl() {
		return sitaRl;
	}

	public void setSitaRl(double sitaRl) {
		this.sitaRl = sitaRl;
	}

	public double getSitaRr() {
		return sitaRr;
	}

	public void setSitaRr(double sitaRr) {
		this.sitaRr = sitaRr;
	}

	//slot number configured on each node
	private int mapSlot;
	private int reduceSlot;
	private int nodeNum;
	private int rackNum;
	
	//all the slots
	private List<DataNode> mapNodes = new ArrayList<DataNode>();
	private List<DataNode> reduceNodes = new ArrayList<DataNode>();
	
	//topology of net:1 local; 2 rack; 3 remote
	private byte[][] topo;
	
	private double omega;
	
	public int getNodeNum() {
		return nodeNum;
	}

	public void setNodeNum(int nodeNum) {
		this.nodeNum = nodeNum;
	}

	public List<DataNode> getMapNodes() {
		return mapNodes;
	}

	public void setMapNodes(List<DataNode> mapNodes) {
		this.mapNodes = mapNodes;
	}

	public List<DataNode> getReduceNodes() {
		return reduceNodes;
	}

	public void setReduceNodes(List<DataNode> reduceNodes) {
		this.reduceNodes = reduceNodes;
	}

	public int getReduceSlot() {
		return reduceSlot;
	}

	public void setReduceSlot(int reduceSlot) {
		this.reduceSlot = reduceSlot;
	}

	public int getMapSlot() {
		return mapSlot;
	}

	public void setMapSlot(int mapSlot) {
		this.mapSlot = mapSlot;
	}

	public byte[][] getTopo() {
		return topo;
	}

	public void setTopo(byte[][] topo) {
		this.topo = topo;
	}

	public int getRackNum() {
		return rackNum;
	}

	public void setRackNum(int rackNum) {
		this.rackNum = rackNum;
	}

	public void setOmega(double omega) {
		this.omega = omega;
	}

	public double getOmega() {
		return omega;
	}

	public Cluster(int node, int rack, int map, int reduce)
	{
		this.nodeNum = node;
		this.rackNum = rack;
		this.setMapSlot(map);
		this.setReduceSlot(reduce);
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
