package data;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.omg.Dynamic.Parameter;

import model.Cluster;
import model.DataNode;
import model.Job;
import model.Schedule;
//the data needed to be generated:
//job/task number, node number, slot number on each node, task duration, data input size, input/output rate
import model.DataNode.Stype;
import model.Task.TaskType;
import model.Task;

public class RandomInstance {

	private static final String INST_PREFIX = "MR_I_BI_NEW_";
	private String instName;
	private Parameters params;
	
	private String outputPath;
	private BufferedWriter writer;
	
	private Random rand = new Random(System.nanoTime());
	
	public RandomInstance(Parameters p, String name)
	{
		this.setParams(p);
		this.instName = INST_PREFIX + name;
		this.outputPath = "..\\TestData\\";
		try {
			this.writer = new BufferedWriter(new FileWriter(outputPath + instName + ".txt", false));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Schedule newInstance() throws IOException
	{
		int jobNum = params.getP_jobs();//20;
		
		int mapTask = 0;
		int reduceTask = 0;
		int taskSize = Cluster.BLOCK_SIZE; //assume now that the input size of each task is equal
		int tsIndex = 0;
		int ioIndex = 0;
		double scale = 0;
		
		int longJob = (int)(jobNum * 0.8);
		for(int i = 1; i <= jobNum; i++)
		{
			if(i <= longJob)
				scale = params.getP_scale_bi_major_low() + rand.nextDouble() * (params.getP_scale_bi_major_high() - 
					params.getP_scale_bi_major_low());
			else
				scale = params.getP_scale_bi_minor_low() + rand.nextDouble() * (params.getP_scale_bi_minor_high() - 
						params.getP_scale_bi_minor_low());
			//mapTask = 5 + rand.nextInt(15);
			NormalDistribution nd = new NormalDistribution(params.getP_map_num_miu(), params.getP_map_num_sig());
			//使用正态分布生成Job i的map task数量
			mapTask = (int)(Math.ceil(nd.getNext()));
			nd = new NormalDistribution(params.getP_reduce_num_miu(), params.getP_reduce_num_sig());	
			//使用正态分布生成Job i的reduce task数量
			reduceTask = (int)Math.ceil(nd.getNext());
			//随机产生作业任务大小索引
			tsIndex = rand.nextInt(Job.task_size.length);
			//随机产生io比率索引
			ioIndex = rand.nextInt(Job.io_rate.length);
			
			//随机产生任务的大小
			taskSize = Job.task_size[tsIndex];

			params.setP_io_rate(Job.io_rate[ioIndex]);

			Job job = new Job(i, mapTask, reduceTask);
			//for deadline
			nd = new NormalDistribution(params.getP_job_deadline_miu(), params.getP_job_deadline_sigma());
			long jobDeadline = (long)(nd.getNext()*scale);
			job.setDeadline(jobDeadline);
			writer.write(mapTask + "\t" + reduceTask + "\t" + taskSize + "\t" + params.getP_io_rate() + "\t" + jobDeadline + "\r\n");
			//s.getJobs().add(job);
			//generate task duration
			//for all map and reduce tasks
			long taskDuration = 0;
			int curSlot = 1;
			for(int j = 1; j <= mapTask; j++)
			{
				nd.setMiu(params.getP_map_dura_miu());
				nd.setSigma(params.getP_map_dura_sig());
				taskDuration = (long)(nd.getNext() * scale);
				
				//每个任务的处理时间
				writer.write(taskDuration + "\t");
			}
			writer.write("\r\n");
			for(int j = 1; j <= reduceTask; j++)
			{
				//taskDuration = 50 + rand.nextInt(100);
				nd.setMiu(params.getP_reduce_dura_miu());
				nd.setSigma(params.getP_reduce_dura_sig());
				taskDuration = (long)(nd.getNext() * scale);
				
				writer.write(taskDuration + "\t");	
			}
			writer.write("\r\n");
			writer.flush();
		}
		
		return null;//s;
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

	public static void main(String[] args) {
		Parameters p = new Parameters();
		p.setP_io_rate(0.1);
		p.setP_jobs(4);
		p.setP_node(100);
		p.setP_rack(10);
		p.setP_map_slot(4);
		p.setP_reduce_slot(2);
		
		p.setP_map_num_miu(40);
		p.setP_map_num_sig(55);
		p.setP_map_dura_miu(50);
		p.setP_map_dura_sig(20);
		
		p.setP_reduce_num_miu(30);
		p.setP_reduce_num_sig(45);
		p.setP_reduce_dura_miu(100);
		p.setP_reduce_dura_sig(30);
		
		//extra add
		p.setP_job_deadline_miu(100);
		p.setP_job_deadline_sigma(50);
		
		p.setP_scale_bi_major_high(2);
		p.setP_scale_bi_major_low(1);
		
		p.setP_scale_bi_minor_high(10);
		p.setP_scale_bi_minor_low(8);
		
		RandomInstance ri = new RandomInstance(p, p.getP_jobs() + "_1");
		//产生30个实例
		int replica = 30;
		int[] jobs = {20,50,100,150,200,250};
		for(int job: jobs)
		{
			//设置每一个的作业数量
			p.setP_jobs(job);
			for(int i = 1; i <= replica; i++)
			{
				ri = new RandomInstance(p,  p.getP_jobs() + "_" + i);
				try {
					ri.newInstance();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			System.out.println("已经生成规模为" + p.getP_jobs() + "的实例" + replica + "个");
		}

	}

}
