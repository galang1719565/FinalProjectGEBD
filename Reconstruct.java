package JcomeJulian;

import java.util.ArrayList;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

public class Reconstruct implements Function<
	Tuple2<Tuple2<String,String>, Tuple4<ArrayList<String>,Integer,String,ArrayList<Tuple2<String,String>>>>,
	Protein> {

	@Override
	public Protein call(Tuple2<Tuple2<String,String>, Tuple4<ArrayList<String>,Integer,String,ArrayList<Tuple2<String,String>>>> six) throws Exception {
		Tuple3<String,String,ArrayList<String>> Key = 
				new Tuple3<String,String,ArrayList<String>>(six._1._1,six._1._2,six._2._1());
		Tuple3<Integer,String,ArrayList<Tuple2<String,String>>> Value = 
				new Tuple3<Integer,String,ArrayList<Tuple2<String,String>>> (six._2._2(),six._2._3(),six._2._4());
		Protein p = new Protein(Key,Value);
		return p;
	}

}
