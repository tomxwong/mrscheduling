package data;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

import model.Cluster;
import model.DataNode;
import model.Job;
import model.Schedule;
//the data needed to be generated:
//job/task number, node number, slot number on each node, task duration, data input size, input/output rate
import model.DataNode.Stype;
import model.Task.TaskType;
import model.Task;
import model.Slot;

public class RandomInstanceFile {

	private static final String INST_PREFIX = "MR_I_BI_";
	private String instName;
	private Parameters params;
	
	private String inputPath;
	//private BufferedWriter writer;
	private BufferedReader reader;
	
	public BufferedReader getReader() {
		return reader;
	}

	public void setReader(BufferedReader reader) {
		this.reader = reader;
	}

	private Schedule result;
	
	private Random rand = new Random(System.nanoTime());
	
	public RandomInstanceFile(Parameters p, String input)
	{
		this.setParams(p);
		//this.instName = INST_PREFIX + name;
		this.inputPath = input;
		try {
			this.reader = new BufferedReader(new FileReader(inputPath + "MR_I_BI_NEW_20_1.txt"));
			//this.writer = new BufferedWriter(new FileWriter(outputPath + instName + ".txt", false));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Schedule newInstance() throws IOException
	{
		String jobs = reader.readLine();
		
		int nodeNum = params.getP_node();//10;
		int mapSlot = params.getP_map_slot();//4;
		int reduceSlot = params.getP_reduce_slot();//2;
		int rackNum = params.getP_rack();//2;
		//int jobNum = params.getP_jobs();//20;
		
		int jobNum = Integer.parseInt(jobs);
		//write to instance file for reuse
		//writer.write(nodeNum + "\t" + rackNum + "\t" + mapSlot + "\t" + reduceSlot + "\t" + jobNum + "\r\n");
		//writer.flush();
		
		//int mapInput = 10000;//unit is metabyte
		//double ioRate = 0.1;//input is 10, output is 1
	
		Cluster cluster = new Cluster(nodeNum, rackNum, mapSlot, reduceSlot);
		cluster.setOmega(params.getP_omega());
		Schedule s = new Schedule(cluster);
		//Schedule sclone = new Schedule(cluster);
		//generate slots info
		//遍历所有的map nodes
		for(int i = 0; i < nodeNum; i++)
		{
			DataNode node = new DataNode(i + 1, Stype.Map);
			cluster.getMapNodes().add(node);
		}
		//为每一个节点增加4个slot
		for (int i = 0; i < nodeNum; i++) {
			for(int j = 0; j < mapSlot; j++){
				cluster.getMapNodes().get(i).getSlots().add(new Slot(j + 1));
				cluster.getMapNodes().get(i).getSlots().get(j).setNodeID(i + 1);
			}
		}
		//遍历所有的reduce node
		for(int i = 0; i < nodeNum ; i++)
		{
			DataNode node = new DataNode(cluster.getMapNodes().size() + i + 1, Stype.Reduce);
			cluster.getReduceNodes().add(node);
		}
		//为每一个节点增加2个slot
		for (int i = 0; i < nodeNum; i++) {
			for(int j = 0; j < reduceSlot; j++){
				cluster.getReduceNodes().get(i).getSlots().add(new Slot(j + 1));
				cluster.getReduceNodes().get(i).getSlots().get(j).setNodeID(i + 1);
			}
		}
		//map node + reduce node 成为矩阵大小 10 + 10   从1开始
		int len = cluster.getMapNodes().size() + cluster.getReduceNodes().size();
		byte[][] topo = new byte[len + 1][];
		for(int i = 0; i < topo.length; i++)
			topo[i] = new byte[len + 1];
		
		//put slot on node in a round-robin way
		//一个机架中节点的数量
		int rackNode = cluster.getNodeNum() / cluster.getRackNum();//+ 1;
		for(int i = 1; i <= len; i++){
			for(int j = 1; j <= len; j++){
				int jvalue = j % cluster.getNodeNum() == 0 ? cluster.getNodeNum() : j % cluster.getNodeNum();
			    int ivalue = i % cluster.getNodeNum() == 0 ? cluster.getNodeNum() : i % cluster.getNodeNum();
				if(ivalue == jvalue)
					topo[i][j] = 1;
				else if(i < j){
					//机架编号X
					ivalue = (ivalue % rackNode == 0 || (ivalue / rackNode + 1) * rackNode > nodeNum) ? ivalue / rackNode - 1 : ivalue / rackNode;
					//机架编号Y
					jvalue = (jvalue % rackNode == 0 || (jvalue / rackNode + 1) * rackNode > nodeNum) ? jvalue / rackNode - 1 : jvalue / rackNode;
					if(ivalue == jvalue)
						topo[i][j] = 2;
					else topo[i][j] = 3;
				}
				else 
					topo[i][j] = topo[j][i];
			}
		}
		///////////////////////////////////////////////////////
		System.out.println("Node matrix is:\n");
		for(int i  = 1; i <= len; i++){
			for(int j = 1; j <= len; j++){
				System.out.print(topo[i][j]+" ");
			}
			System.out.println("");
		}
		//////////////////////////////////////////////////////
		cluster.setTopo(topo);
		//generate task info according to the probability distribution
		//here
		//
		//s.setIoRate(params.getP_io_rate());
		//sclone.setIoRate(params.getP_io_rate());
		int mapTask = 0;
		int reduceTask = 0;
		int deadLine = 0;
		int taskSize = Cluster.BLOCK_SIZE; //assume now that the input size of each task is equal
		double ioRate = 0.1;
		String tasks = null;
		for(int i = 1; i <= jobNum; i++)
		{
			//mapTask = 5 + rand.nextInt(15);
			//NormalDistribution nd = new NormalDistribution(params.getP_map_num_miu(), params.getP_map_num_sig());
			//mapTask = (int)Math.ceil(nd.getNext());
			tasks = reader.readLine();
			String[] taskInfo = tasks.split("\t");
			//获取map task数量
			mapTask = Integer.parseInt(taskInfo[0]);
			
			//nd = new NormalDistribution(params.getP_reduce_num_miu(), params.getP_reduce_num_sig());
			//reduceTask = (int)Math.ceil(nd.getNext());
			//获取reduce task数量
			reduceTask = Integer.parseInt(taskInfo[1]);
			//获取任务数据大小
			taskSize = Integer.parseInt(taskInfo[2]);
			//获取io比率
			ioRate = Double.parseDouble(taskInfo[3]);
			//获取deadline
			deadLine = Integer.parseInt(taskInfo[4]);
			params.setP_io_rate(ioRate);
			
			//writer.write(mapTask + "\t" + reduceTask + "\r\n");
			
			//mapTask = mapInput / Cluster.BLOCK_SIZE + 1;
			//int mapInput = mapTask * Cluster.BLOCK_SIZE;//for simplicity and better data locality:split size == block size
			//taskSize = Cluster.BLOCK_SIZE;
			
			Job job = new Job(i, mapTask, reduceTask);
			job.setDeadline(deadLine);
			//
			System.out.println("generate job id:"+i+" map task num: "+job.getMapNum()+" reduce task num: "+job.getReduceNum());
			//
			//s.getJobs().add(job);
			//generate task duration
			//for all map and reduce tasks
			long taskDuration = 0;
			//int curSlot = 1;
			int curNode = 1;
			String durations = reader.readLine();
			String[] mapDur = durations.split("\t");
			for(int j = 1; j <= mapTask; j++)
			{

				taskDuration = Long.parseLong(mapDur[j - 1]);
				
				Task task = new Task(j, taskDuration, TaskType.MAP, job);
				//
				System.out.println(" genrate task id:"+j+" taskDuration:"+taskDuration+" task type:"+TaskType.MAP+" job id:"+job.getJobID());
				//
				//每个任务的输入数据大小相同
				task.setInputSize(taskSize);
				task.setOutputSize((task.getInputSize() * params.getP_io_rate()));
				task.setJobID(i);

				curNode = setDataDistribution(task, curNode, cluster);
				
				job.getMaps().add(task);
				s.getMaps().add(task);
				//sclone.getMaps().add(task);
			}
			//writer.write("\r\n");
			durations = reader.readLine();
			String[] reduceDur = durations.split("\t");
			for(int j = 1; j <= reduceTask; j++)
			{
				taskDuration = Long.parseLong(reduceDur[j - 1]);
				//writer.write(taskDuration + "\t");
				
				Task task = new Task(j, taskDuration, TaskType.REDUCE, job);
				//
				System.out.println(" genrate task id:"+j+" taskDuration:"+taskDuration+" task type:"+TaskType.REDUCE+" job id:"+job.getJobID());
				//
				//here get the input size of reduce task: the sum of all map tasks of the same job
				task.setInputSize(task.getJob().getMaps().get(0).getOutputSize() * 1.0
						/ task.getJob().getReduceNum() * task.getJob().getMapNum());
				task.setJobID(i);
				job.getReduces().add(task);
				s.getReduces().add(task);
				//sclone.getReduces().add(task);
			}

			s.getJobs().add(job);
			//sclone.getJobs().add(job);
		}

		//
		for (Job job : s.getJobs()) {
			for (Task task : job.getMaps())
				for (int it : task.getDataLocations().keySet())
					//System.out.println("job id: "+job.getJobID()+" task id: "+task.getTaskID()+" data node id: "+it+" dataSize: "+task.getDataLocations().get(it));
			          System.out.println("job id: "+job.getJobID()+" task id: "+task.getTaskID()+" task type: "+task.getType()+" dataLocationsSize: "+task.getDataLocations().size()+" node id: "+it+" data size: "+task.getDataLocations().get(it));
			for (Task task : job.getReduces())
				for (int it : task.getDataLocations().keySet())
					//System.out.println("job id: "+job.getJobID()+" task id: "+task.getTaskID()+" data node id: "+it+" dataSize: "+task.getDataLocations().get(it));
					System.out.println("job id: "+job.getJobID()+" task id: "+task.getTaskID()+" task type: "+task.getType()+" dataLocationsSize: "+task.getDataLocations().size()+" node id: "+it+" data size: "+task.getDataLocations().get(it));
			}
		this.setResult(s);
		return s;
	}

	private int setDataDistribution(Task task, int curNode, Cluster cluster) {
	int interval = Cluster.DUPLICATE;
	int i;
	for(i = 1; i < curNode + interval/*cluster.getMapNodes().size()*/; i++)
	{
		if(i >= curNode){
			task.getDataLocations().put(i % cluster.getMapNodes().size() == 0 ? cluster.getMapNodes().size() : 
				i % cluster.getMapNodes().size(), (int)task.getInputSize());
			task.getHostList().add(i);
		}
		else
			task.getDataLocations().put(i, 0);
		
	}
	if(i <= cluster.getMapNodes().size()){
		for(;i <= cluster.getMapNodes().size(); i++)
			task.getDataLocations().put(i, 0);
	}
	curNode = (curNode + interval) % cluster.getMapNodes().size();
	return curNode;	
	}
	public Parameters getParams() {
		return params;
	}

	public void setParams(Parameters params) {
		this.params = params;
	}

	public void setInstName(String instName) {
		this.instName = instName;
	}

	public String getInstName() {
		return instName;
	}

	public void setResult(Schedule result) {
		this.result = result;
	}

	public Schedule getResult() {
		return result;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Parameters p = new Parameters();
		p.setP_io_rate(0.1);
		p.setP_jobs(4);
		p.setP_node(20);
		p.setP_rack(2);
		p.setP_map_slot(4);
		p.setP_reduce_slot(2);
		
		p.setP_map_num_miu(15);
		p.setP_map_num_sig(55);
		p.setP_map_dura_miu(5);
		p.setP_map_dura_sig(20);
		
		p.setP_reduce_num_miu(2);
		p.setP_reduce_num_sig(14);
		p.setP_reduce_dura_miu(10);
		p.setP_reduce_dura_sig(30);
		
		//extra add
		p.setP_job_deadline_miu(200);
		p.setP_job_deadline_sigma(100);
		
		RandomInstanceFile ri = new RandomInstanceFile(p, "..\\TestData\\");
		try {
			Schedule s = ri.newInstance();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
