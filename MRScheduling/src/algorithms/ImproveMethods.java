package algorithms;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import model.DataNode;
import model.Schedule;
import model.SlotComparator;
import model.Task;

public class ImproveMethods {

	public static void improve(Schedule schedule)
	{
		ArrayList<DataNode> slots = schedule.getCurAvailList();
		Collections.sort(slots, new SlotComparator());
		
		int count = 0;
		int maxIter = 10;
		while (count < maxIter) {
			DataNode shortest = slots.get(0);
			DataNode longest = slots.get(slots.size() - 1);

			//long stime = shortest.getCurFinishTime();
			//long ltime = longest.getCurFinishTime();

			int sp = shortest.getTasks().size();
			int sr = longest.getTasks().size();

			// Collections.sort(longest.getTasks(), new TaskComparator());
			// Collections.sort(shortest.getTasks(), new TaskComparator());
			int si = getShortestTask(shortest.getTasks());
			int li = getLongestTask(longest.getTasks());
			Task ts = shortest.getTasks().remove(si);
			Task tl = longest.getTasks().remove(li);
			long oldsSetup = ts.getSetupTime(); 
			long oldlSetup = tl.getSetupTime();
			
			
			schedule.calculateSetupTime(ts, longest);
			long oldStart = ts.getStartTime();
			ts.setStartTime(tl.getStartTime());
			ts.setFinishTime(ts.getStartTime() + ts.getProcessTime() + ts.getSetupTime());
			schedule.calculateSetupTime(tl, shortest);
			tl.setStartTime(oldStart);
			tl.setFinishTime(tl.getStartTime() + tl.getProcessTime() + tl.getSetupTime());
			
			long diffs = tl.getProcessTime() - ts.getProcessTime()
					+ tl.getSetupTime() - oldsSetup;
			
			long diffl = tl.getProcessTime() - ts.getProcessTime()
			+ oldlSetup - ts.getSetupTime();
			
			if (diffl < 0 || diffs < 0)
				break;

			long gain = 0; 
			long loss = 0;
			
			for (int i = si; i < shortest.getTasks().size(); i++) {
				Task t = shortest.getTasks().get(i);
				long old = t.getJob().getMapFinishTime();
				t.setFinishTime(t.getFinishTime() + diffs);
				t.setStartTime(t.getStartTime() + diffs);

				if (t.getFinishTime() > old){
					t.getJob().setMapFinishTime(t.getFinishTime());
					loss += (t.getJob().getMapFinishTime() - old);
				}

			}
			for (int i = li; i < longest.getTasks().size(); i++) {
				Task t = longest.getTasks().get(i);
				//long old = t.getFinishTime();
				t.setFinishTime(t.getFinishTime() - diffl);
				t.setStartTime(t.getStartTime() - diffl);

				//if (old == t.getJob().getMapFinishTime()){
					//t.getJob().setMapFinishTime(t.getFinishTime());
					//gain += (old - t.getJob().getMapFinishTime());
				//}
				//gain += (old - t.getJob().getMapFinishTime());
				long oldMapFinish = t.getJob().getMapFinishTime();
				if(t.getFinishTime() < t.getJob().getMapFinishTime())
				{
					long max = Long.MIN_VALUE;
					for (int k = 0; k < t.getJob().getMaps().size(); k++)
						if (t.getFinishTime() > max)
							max = t.getJob().getMaps().get(k).getFinishTime();
					t.getJob().setMapFinishTime(max);
				}
				if(oldMapFinish > t.getJob().getMapFinishTime())
					gain += (oldMapFinish - t.getJob().getMapFinishTime());
				

			}
			//tl.setStartTime(shortest.getTasks().get(sp - 2).getFinishTime());
		   // tl.setFinishTime(tl.getStartTime() + tl.getProcessTime() + tl.getSetupTime());
		    
		    
		    if(ts.getFinishTime() > ts.getJob().getMapFinishTime())
		    {
		    	loss += (ts.getFinishTime() - ts.getJob().getMapFinishTime());
		    	ts.getJob().setMapFinishTime(ts.getFinishTime());
		    	
		    }
		    if(tl.getFinishTime() > tl.getJob().getMapFinishTime()){
		    	loss += (tl.getFinishTime() - tl.getJob().getMapFinishTime());
		    	tl.getJob().setMapFinishTime(tl.getFinishTime());
		    }
		    	
		    
			shortest.getTasks().add(si, tl);
			shortest.setCurFinishTime(shortest.getTasks().get(sp - 1).getFinishTime());
			longest.getTasks().add(li, ts);
			longest.setCurFinishTime(longest.getTasks().get(sr - 1).getFinishTime());
			
			Collections.sort(slots, new SlotComparator());
			
			if(gain <= loss)
				count++;
			else{
				System.out.println("gain: " + (gain - loss));
				break;
			}

			
		}
	}
	private static int getLongestTask(List<Task> tasks) {
		long max = Long.MIN_VALUE;
		int index = 0;
		for(int i = 0; i < tasks.size(); i++){
			if(tasks.get(i).getProcessTime() + tasks.get(i).getSetupTime() > max)
			{
				max = tasks.get(i).getProcessTime() + tasks.get(i).getSetupTime();
				index = i;
			}
		}
		return index;
	}
	private static int getShortestTask(List<Task> tasks) {
		long min = Long.MAX_VALUE;
		int index = 0;
		for(int i = 0; i < tasks.size(); i++){
			if(tasks.get(i).getProcessTime() + tasks.get(i).getSetupTime() < min)
			{
				min = tasks.get(i).getProcessTime() + tasks.get(i).getSetupTime();
				index = i;
			}
		}
		return index;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
