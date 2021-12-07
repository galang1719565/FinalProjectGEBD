package JcomeJulian;

import java.util.ArrayList;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class Check implements Function
<ArrayList<Tuple2<String,String>>,
ArrayList<Tuple2<String,String>> > {

	@Override
	public ArrayList<Tuple2<String, String>> call(ArrayList<Tuple2<String, String>> EdgesComp) throws Exception {
		
		
		ArrayList<Tuple2<String, String>> h = new ArrayList<Tuple2<String, String>>(EdgesComp);
		ArrayList<Tuple2<String, String>> results = new ArrayList<Tuple2<String, String>>();
		
//		System.out.println("\n\nCiclo WHILE\t\th: "+h);
		Tuple2<String,String> e = h.get(0); h.remove(0);
		if(!check(e,h))results.add(e);
		while(!h.isEmpty()) {
			e = h.get(0); h.remove(0);
//			System.out.println("e:"+e+", h: "+h);
			if(!h.isEmpty()&&check(e,h)) {
//				System.out.println("CHEEEEEEEEEEECK\te "+e+"\th "+h);
			}
			else results.add(e);
//			System.out.println("results: "+results);
		}
		return results;
	}

	private boolean check(Tuple2<String, String> e, ArrayList<Tuple2<String, String>> list) {
		boolean result = false;
		ArrayList<Tuple2<String,String>> list1 = new ArrayList<Tuple2<String,String>>(list);
		while(result!=true&&!list1.isEmpty()) {
			Tuple2<String, String> arco = list1.get(0);
			if(e._1.equals(arco._1)&&e._2.equals(arco._2)) result=true;
			if(e._2.equals(arco._1)&&e._1.equals(arco._2)) result=true;
			list1.remove(arco);
		}
		return result;
	}
}
