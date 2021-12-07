package JcomeJulian;

import java.util.ArrayList;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import scala.Tuple4;

public class getNeigh implements
		Function2<Tuple4<ArrayList<String>, Integer, String, ArrayList<Tuple2<String, String>>>, Tuple4<ArrayList<String>, Integer, String, ArrayList<Tuple2<String, String>>>, Tuple4<ArrayList<String>, Integer, String, ArrayList<Tuple2<String, String>>>> {

	@Override
	public Tuple4<ArrayList<String>, Integer, String, ArrayList<Tuple2<String, String>>> call(
			Tuple4<ArrayList<String>, Integer, String, ArrayList<Tuple2<String, String>>> p1,
			Tuple4<ArrayList<String>, Integer, String, ArrayList<Tuple2<String, String>>> p2) throws Exception {
		if(p1._3().equals("BLACK")) return p1;
		if(p2._3().equals("BLACK")) return p2;
		if(p1._1()==null||p1._1().isEmpty()) {
			Tuple4<ArrayList<String>,Integer,String,ArrayList<Tuple2<String,String>>> result = 
					new Tuple4<ArrayList<String>,Integer,String,ArrayList<Tuple2<String,String>>>
						(p2._1(),p1._2(),p1._3(),p1._4());
//			System.out.println("\n\ngetNeigh: "+p1._4());
		
			return result;
		}
		else {
			Tuple4<ArrayList<String>,Integer,String,ArrayList<Tuple2<String,String>>> result = 
					new Tuple4<ArrayList<String>,Integer,String,ArrayList<Tuple2<String,String>>>
						(p1._1(),p2._2(),p2._3(),p2._4());
//			System.out.println("\n\ngetNeigh: "+p1._4());

			return result;
		}
	}	


	
}
