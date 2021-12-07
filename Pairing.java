package JcomeJulian;

import java.util.*;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;



public class Pairing  implements PairFunction<
	Protein,Tuple2<String,String>, 
	Tuple4<ArrayList<String>,Integer,String,ArrayList<Tuple2<String,String>>> > {

	public Tuple2 call(Protein p) throws Exception {
		Tuple3<String,String,ArrayList<String>> key = p.getKey();
		Tuple2<String,String> pair = new Tuple2<String,String>(key._1(),key._2());
		Integer val1 = p.getValue()._1();
		String val2 = p.getValue()._2();
		ArrayList<Tuple2<String,String>> val3 = p.getValue()._3();
		Tuple4<ArrayList<String>,Integer,String,ArrayList<Tuple2<String,String>>> value = 
				new Tuple4<ArrayList<String>,Integer,String,ArrayList<Tuple2<String,String>>>
				(p.getKey()._3(),val1,val2,val3);
	return new Tuple2<Tuple2<String,String>, Tuple4<ArrayList<String>,Integer,String,ArrayList<Tuple2<String,String>>>>
		(pair, value);}

	
}
