package algorithms;

import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.apache.commons.beanutils.BeanUtils;


public class TEST{
	TEST(){}
	private class A{
		B bm;
		A(){}
		A(A a){
			this.bm = new B();
		}
	};
	private class B{
		B(){}
	};
	public void f(A a){
		A aa = a;
		System.out.println("aa:"+aa.hashCode());
		System.out.println("a"+a.hashCode());
	}
	private static class ListComparator implements Comparator<Object>{
		@SuppressWarnings("unchecked")
		@Override
		public int compare(Object o1, Object o2) {
			// TODO Auto-generated method stub
			List<Integer> list1 = (List<Integer>)o1;
			List<Integer> list2 = (List<Integer>)o2;
			
			long r1 = list1.size(),r2 = list2.size();
			if(r1 > r2){
				return 1;
			}else if(r1 == r2){
				return 0;
			}else{
				return -1;
			}
		}
	}
	public int[] getN(int num){
		Random r = new Random(System.currentTimeMillis());
		int[] arr = new int[num];
		for(int i = 0; i < num; ++i){
			arr[i]=i;
		}
		for(int i = num - 1;i >= 0; --i){
			int temp = arr[i];
			int tepIndex = r.nextInt(num);
			arr[i] = arr[tepIndex];
			arr[tepIndex] = temp;
		}
		return arr;
	}
	public static void main(String[] args) throws IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException{
		TEST test = new TEST();
//		A a1 = test.new A();
//		A a2 = test.new A(a1);
//		System.out.println(a1.bm == a2.bm);
		List<Integer> ll = new ArrayList<Integer>();
		ll.add(1);
		ll.add(2);
		ll.add(3);
//		System.out.println(ll.subList(0, 2));
//		List<List<Integer>> ls = new ArrayList<List<Integer>>();
		List<Integer> ls1 = (ArrayList<Integer>) BeanUtils.cloneBean(ll);
		ll.set(0, 12);
		
		System.out.println(ll.get(0));
		System.out.println(ls1.size());
//		ls1.add(2);
//		ls1.add(3);
//		List<Integer> ls2 = new ArrayList<Integer>();
//		ls2.add(4);
//		List<Integer> ls3 = new ArrayList<Integer>();
//		ls3.add(4);
//		ls3.add(5);
//		ls.add(ls1);
//		ls.add(ls2);
//		ls.add(ls3);
//		Collections.sort(ls, new ListComparator());
//		System.out.println("hi");
//		long t = System.currentTimeMillis();
//      Random r = new Random(t);
//      System.out.println(r.nextInt()%10);
//		int [] aa = test.getN(10);
//		System.out.println(aa.toString());
//		SimpleDateFormat time=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//		String TimeString = time.format(new java.util.Date());
//		System.out.println(TimeString);
	}
};