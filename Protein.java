package JcomeJulian;

import java.util.*;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;


public class Protein implements Serializable{

		public String NodeId;
		public String Root;
		public ArrayList<String> Neighbors;
		public Integer Distance;		
		public String Color;
		public ArrayList<Tuple2<String,String>> Path;
		public Tuple3<String,String,ArrayList<String>> Key;
		public Tuple3<Integer,String,ArrayList<Tuple2<String,String>>> Value;
		

		public Protein(Tuple3<String,String,ArrayList<String>> key, Tuple3<Integer,String,ArrayList<Tuple2<String,String>>> value) {
			this.Key = key;
			this.Value = value;
		}


		public Tuple3<String, String, ArrayList<String>> getKey() {
			return Key;
		}
		public Tuple3<Integer,String,ArrayList<Tuple2<String,String>>> getValue() {
			return Value;
		}


		public void setKey(Tuple3<String, String, ArrayList<String>> key) {
			Key = key;
		}
		public void setValue(Tuple3<Integer,String,ArrayList<Tuple2<String,String>>> value) {
			Value = value;
		}

				
		
		
}
		


