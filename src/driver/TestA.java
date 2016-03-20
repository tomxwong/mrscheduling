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

public class TestA {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Parameters p = new Parameters();
		// p.setP_io_rate(0.1);
		p.setP_jobs(20);
		p.setP_node(20);
		p.setP_rack(3);
		p.setP_map_slot(4);
		p.setP_reduce_slot(2);

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

		String path = "G:\\Research\\Data\\";
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(
					"G:\\Research\\Data\\Results\\" + p.getP_jobs() + "_"
							+ p.getP_node() + "_JR.txt", false));
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
			int[] nodes = {10,15,25,30,35};
			int instNum = 30;
			for (int node : nodes) {
				System.out.println("--------------------------node :" + node
						+ "--------------------------------");
				p.setP_node(node);
				writer = new BufferedWriter(new FileWriter(
						"G:\\Research\\Data\\Results\\" + "4DUP"
								+ "_" + p.getP_node() + ".txt", false));
				for (int s : size) {
					System.out.println("--------------------------job :" + s
							+ " node = " + node +"--------------------------------");

					p.setP_jobs(s);
					/*writer = new BufferedWriter(new FileWriter(
							"G:\\Research\\Data\\Results\\" + p.getP_jobs()
									+ "_" + p.getP_node() + "_odd.txt", false));*/
					inst = 0;
					int count  = 0;
					double sum1 = 0;
					double sum2 = 0;
					double sum3 = 0;
					double sum4 = 0;
					double sum5 = 0;
					double sum6 = 0;
					for (String file : fileList) {
						if (file.contains("_BI_LPT_" + p.getP_jobs() + "_")) {
							System.out.println("Instance: " + file);
							rif1 = new RandomInstanceFile(p, path + file);
							s1 = rif1.newInstance();
							s1.getLowerBound();

							DefaultMethod dm1 = new DefaultMethod(s1, "JR-LPT");
							dm1.init();
							dm1.start();
							dm1.output();
							//rif1.setResult(new Schedule(s1));
							
							rif1 = new RandomInstanceFile(p, path + file);
							s2 = new Schedule(s1);//rif1.newInstance();
							s2.getLowerBound();

							SlotGreedyHeuristic dm2 = new SlotGreedyHeuristic(s2, "JR-LPT");
							dm2.init();
							dm2.start();
							dm2.output();
							
							rif1 = new RandomInstanceFile(p, path + file);
							s3 = new Schedule(s1);//rif1.newInstance();
							s3.getLowerBound();

							LPTTaskGreedy gh = new LPTTaskGreedy(s3, "LPT-TASK");
							gh.init();
							gh.start();
							gh.output();
							
							rif1 = new RandomInstanceFile(p, path + file);
							s4 = new Schedule(s1);//rif1.newInstance();
							s4.getLowerBound();

							SlotGreedyHeuristicLocal gh1 = new SlotGreedyHeuristicLocal(s4, "JR-LPT");
							gh1.init();
							gh1.start();
							gh1.output();
							
							rif1 = new RandomInstanceFile(p, path + file);
							s5 = new Schedule(s1);//rif1.newInstance();
							s5.getLowerBound();

							LPTTaskGreedyLocal gh2 = new LPTTaskGreedyLocal(s5, "LPT-TASK");
							gh2.init();
							gh2.start();
							gh2.output();
							
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

							/*writer.write(++inst + "\t" + s2.lowerBound() + "\t"
									//+ s1.getMakespan() + "\t\t"
									+ s2.getMakespan() + "\t\t"
									+ s3.getMakespan() + "\r\n");
									//+ s4.getMakespan() + "\t\t"
									//+ s5.getMakespan() + "\r\n");
							writer.flush();*/
							sum1 += (s1.getMakespan() - s1.lowerBound()) * 1.0 / s1.lowerBound() ;
							sum2 += (s2.getMakespan() - s1.lowerBound()) * 1.0 / s1.lowerBound() ;
							sum3 += (s3.getMakespan() - s1.lowerBound()) * 1.0 / s1.lowerBound() ;
							sum4 += (s4.getMakespan() - s1.lowerBound()) * 1.0 / s1.lowerBound() ;
							sum5 += (s5.getMakespan() - s1.lowerBound()) * 1.0 / s1.lowerBound() ;
							//sum6 += (s6.getMakespan() - s1.lowerBound()) * 1.0 / s1.lowerBound() ;
							//if(s5.getMakespan() <= s4.getMakespan())count++;
						}
					}
					writer.write(sum1 * 100.0 / instNum    + "\t" + sum2 * 100.0 / instNum  + "\t" + 
							sum3 * 100.0 / instNum    + "\t" + sum4 * 100.0 / instNum  + "\t" + 
							sum5 * 100.0 / instNum    /*+ "\t" + sum6 * 100.0 / instNum*/  + "\r\n");
					writer.flush();
					//writer.close();
				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
