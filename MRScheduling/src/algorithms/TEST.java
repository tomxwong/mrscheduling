package algorithms;

import java.util.ArrayList;
import java.util.List;

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
	public static void main(String[] args){
//		TEST test = new TEST();
//		A a1 = test.new A();
//		A a2 = test.new A(a1);
//		System.out.println(a1.bm == a2.bm);
		List<Integer> ll = new ArrayList<Integer>();
		ll.add(1);
		ll.add(2);
		ll.add(3);
		System.out.println(ll.subList(0, 2));
		
	}
};