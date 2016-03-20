package driver;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import model.Schedule;
import algorithms.DefaultMethod;
import algorithms.LPTTaskGreedy;
import algorithms.LPTTaskGreedyLocal;
import algorithms.SlotGreedyHeuristic;
import algorithms.SlotGreedyHeuristicLocal;
import data.Parameters;
import data.RandomInstanceFile;

public class TestNew {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Parameters p = new Parameters();
		// p.setP_io_rate(0.1);
		p.setP_jobs(20);
		p.setP_node(20);
		p.setP_rack(3);
		//p.setP_map_slot(4);
		p.setP_reduce_slot(2);
		p.setP_omega(0.5);

		p.setP_map_num_miu(154);
		p.setP_map_num_sig(558);
		p.setP_map_dura_miu(50);
		p.setP_map_dura_sig(200);

		p.setP_reduce_num_miu(19);
		p.setP_reduce_num_sig(145);
		p.setP_reduce_dura_miu(100);
		p.setP_reduce_dura_sig(300);

		p.setP_scale_bi_major_high(2);
		p.setP_scale_bi_major_low(1);

		p.setP_scale_bi_minor_high(10);
		p.setP_scale_bi_minor_low(8);

		String path = "G:\\Research\\Data\\NewData\\";
		try {
			BufferedWriter dataWriter = null;/*new BufferedWriter(new FileWriter(
					"G:\\Research\\Data\\Results\\NewResults\\" + p.getP_jobs() + "_"
							+ p.getP_node() + "_Methods_Data.txt", false));*/
			BufferedWriter presentWriter = null;/*new BufferedWriter(new FileWriter(
					"G:\\Research\\Data\\Results\\NewResults\\" + p.getP_jobs() + "_"
							+ p.getP_node() + "_Methods_Present.txt", false));*/
			File dir = new File(path);
			String[] fileList = dir.list();

			int inst = 0;
			RandomInstanceFile rif1 = null;
			RandomInstanceFile rif2 = null;
			RandomInstanceFile rif3 = null;
			RandomInstanceFile rif4 = null;
			RandomInstanceFile rif5 = null;
			RandomInstanceFile rif6 = null;
			Schedule s1 = null;
			Schedule s2 = null;
			Schedule s3 = null;
			Schedule s4 = null;
			Schedule s5 = null;
			Schedule s6 = null;

			int[] size = {20, 50, 100, 150, 200, 250};
			int[] nodes = {/*12,16,20,24,28,32*/10,15,20,25,30,35};
			double[] w = {0.1,0.3,0.7,0.9};
			int[] slotFactor = {1,3,4};
			int instNum = 30;
			for(int multiple : slotFactor)
			{
				
				p.setP_map_slot(p.getP_reduce_slot() * multiple);
				System.out.println("--------------------------rate :" + multiple
						+ "--------------------------------");
			for (int node : nodes) {
				System.out.println("--------------------------Rate: " + multiple + " node: " + node
						+ "--------------------------------");
				p.setP_node(node);
				dataWriter = new BufferedWriter(new FileWriter(
						"G:\\Research\\Data\\Results\\NewResults\\"
								+ p.getP_node() + "_" + multiple + "_Methods_Data_MRRate.txt", false));
				presentWriter = new BufferedWriter(new FileWriter(
						"G:\\Research\\Data\\Results\\NewResults\\"
								+ p.getP_node() + "_" + multiple + "_Methods_Present_MRRate.txt", false));
				dataWriter.write("Instance\tLowerBound\tEATime\tEFTime\tLPTTime\tEFLTime\tLPTLTime\t\tEA\tEF\tLPT\tEFL\tLPTL\r\n");
				presentWriter.write("Instance\tEA\tEF\tLPT\tEFL\tLPTL\tLPTCount\tEATime\tEFTime\tLPTTime\tEFLTime\tLPTLTime\r\n");
				for (int s : size) {
					System.out.println("--------------------------job : " + s
							+ " node : " + node +"--------------------------------");

					p.setP_jobs(s);
					/*writer = new BufferedWriter(new FileWriter(
							"G:\\Research\\Data\\Results\\" + p.getP_jobs()
									+ "_" + p.getP_node() + "_odd.txt", false));*/
					presentWriter.write(node + "_" + s + "\t");
					inst = 0;
					int countLPT  = 0;
					double sum1 = 0;
					double sum2 = 0;
					double sum3 = 0;
					double sum4 = 0;
					double sum5 = 0;
					double time1 = 0,time2 = 0,time3 = 0,time4 = 0,time5 = 0;
					long start = 0;
					long end = 0;
					for (String file : fileList) {
						if (file.contains("_BI_NEW_" + p.getP_jobs() + "_")) {
							System.out.println("Instance: " + file);
						
							
							rif1 = new RandomInstanceFile(p, path + file);
							s1 = rif1.newInstance();
							s1.getLowerBound();
							dataWriter.write(file.substring(0,file.indexOf(".txt")) + "\t" + s1.lowerBound() + "\t");
							
							//DefaultMethod dm1 = new DefaultMethod(s1, "JR-LPT");
							DefaultMethod dm1 = new DefaultMethod(s1, "JR-NEW");
							start = System.currentTimeMillis();
							dm1.init();
							dm1.start();
							end = System.currentTimeMillis();
							time1 += (end - start);
							dm1.output();
							dataWriter.write((end - start) + "\t");
							//rif1.setResult(new Schedule(s1));
							
							rif1 = new RandomInstanceFile(p, path + file);
							s2 = new Schedule(s1);//rif1.newInstance();
							//s2.getLowerBound();

							//SlotGreedyHeuristic dm2 = new SlotGreedyHeuristic(s2, "JR-LPT");
							SlotGreedyHeuristic dm2 = new SlotGreedyHeuristic(s2, "JR-NEW");
							start = System.currentTimeMillis();
							dm2.init();
							dm2.start();
							end = System.currentTimeMillis();
							time2 += (end - start);
							dm2.output();
							dataWriter.write((end - start) + "\t");
							
							rif1 = new RandomInstanceFile(p, path + file);
							s3 = new Schedule(s1);//rif1.newInstance();
							//s3.getLowerBound();

							LPTTaskGreedy gh = new LPTTaskGreedy(s3, "JR-NEW");
							start = System.currentTimeMillis();
							gh.init();
							gh.start();
							end = System.currentTimeMillis();
							time3 += (end - start);
							gh.output();
							dataWriter.write((end - start) + "\t");
							
							rif1 = new RandomInstanceFile(p, path + file);
							s4 = new Schedule(s1);//rif1.newInstance();
							//s4.getLowerBound();

							//SlotGreedyHeuristicLocal gh1 = new SlotGreedyHeuristicLocal(s4, "JR-LPT");
							SlotGreedyHeuristicLocal gh1 = new SlotGreedyHeuristicLocal(s4, "JR-NEW");
							start = System.currentTimeMillis();
							gh1.init();
							gh1.start();
							end = System.currentTimeMillis();
							gh1.output();
							time4 += (end - start);
							dataWriter.write((end - start) + "\t");
							
							rif1 = new RandomInstanceFile(p, path + file);
							s5 = new Schedule(s1);//rif1.newInstance();
							//s5.getLowerBound();

							LPTTaskGreedyLocal gh2 = new LPTTaskGreedyLocal(s5, "JR-NEW");
							start = System.currentTimeMillis();
							gh2.init();
							gh2.start();
							end = System.currentTimeMillis();
							time5 += (end - start);
							gh2.output();
							dataWriter.write((end - start) + "\t\t");
							/*rif1 = new RandomInstanceFile(p, path + file);
							s6 = new Schedule(s1);//rif1.newInstance();
							s6.getLowerBound();

							LPTTaskGreedy gh3 = new LPTTaskGreedy(s6, "JR-NEW");
							gh3.init();
							gh3.start();
							gh3.output();*/

							/*rif2 = new RandomInstanceFile(p, path + file);
							s2 = rif2.newInstance();
							s2.getLowerBound();

							SlotGreedyHeuristic gh = new SlotGreedyHeuristic(
									s2, "JR-LPT");
							DefaultMethod gh = new DefaultMethod(s2, "JR-LPT");*/
							/*LPTTaskGreedy gh = new LPTTaskGreedy(s2, "LPT-TASK");
							gh.init();
							gh.start();
							gh.output();

							rif3 = new RandomInstanceFile(p, path + file);
							s3 = rif3.newInstance();*/

							/*SlotGreedyHeuristic gh1 = new SlotGreedyHeuristic(s3,
									"JR-NEW");*/
							/*DefaultMethod gh1 = new DefaultMethod(s3, "JR-NEW");*/
							/*LPTTaskGreedy gh1 = new LPTTaskGreedy(s3, "JR-NEW");
							gh1.init();
							gh1.start();
							gh1.output();
*/
							/*rif4 = new RandomInstanceFile(p, path + file);
							s4 = rif4.newInstance();
							s4.getLowerBound();

							SlotGreedyHeuristicLocal h2 = new SlotGreedyHeuristicLocal(
									s4, "JR-LPT");
							h2.init();
							h2.start();
							h2.output();

							rif5 = new RandomInstanceFile(p, path + file);
							s5 = rif5.newInstance();

							LPTTaskGreedyLocal h3 = new LPTTaskGreedyLocal(s5,
									"LPT-TASK");
							h3.init();
							h3.start();
							h3.output();*/

							dataWriter.write(s1.getMakespan() + "\t"
									+ s2.getMakespan() + "\t"
									+ s3.getMakespan() + "\t"
									+ s4.getMakespan() + "\t"
									+ s5.getMakespan() + "\r\n");
							dataWriter.flush();
							
							if(s5.getMakespan() < s4.getMakespan())
								countLPT++;
							sum1 += (s1.getMakespan() - s1.lowerBound()) * 1.0 / s1.lowerBound() ;
							sum2 += (s2.getMakespan() - s1.lowerBound()) * 1.0 / s1.lowerBound() ;
							sum3 += (s3.getMakespan() - s1.lowerBound()) * 1.0 / s1.lowerBound() ;
							sum4 += (s4.getMakespan() - s1.lowerBound()) * 1.0 / s1.lowerBound() ;
							sum5 += (s5.getMakespan() - s1.lowerBound()) * 1.0 / s1.lowerBound() ;
							//sum6 += (s6.getMakespan() - s1.lowerBound()) * 1.0 / s1.lowerBound() ;
							//if(s5.getMakespan() <= s4.getMakespan())count++;
						}
					}
					presentWriter.write(sum1 * 100.0 / instNum    + "\t" + sum2 * 100.0 / instNum  + "\t" + 
							sum3 * 100.0 / instNum    + "\t" + sum4 * 100.0 / instNum  + "\t" + 
							sum5 * 100.0 / instNum    + "\t" + countLPT + "\t" + time1 / instNum  + "\t" + time2 / instNum
							+ "\t" + time3 / instNum + "\t" + time4 / instNum + "\t" + time5 / instNum + "\r\n");
					presentWriter.flush();
					//writer.close();
				}
			}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
