package JcomeJulian;

import java.util.*;

import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;

public class ForwardMR implements FlatMapFunction<Protein,Protein> {
		
	public Iterator<Protein> call(Protein p){
		
		
		
		List<Protein> output = new ArrayList<Protein>();
			if(p.getValue()._2().equals("GREY")) {
				if(p.getKey()._3()==null||p.getKey()._3().isEmpty()) {
					//p.setValue(new Tuple3<Integer, String, ArrayList<Tuple2<String,String>>>(p.getValue()._1(),"BLACK",p.getValue()._3()));
					//output.add(p);
					System.out.println("\t\tSHIIIIIIIIIIIT");
				}
				else {
				for(String neigh:p.getKey()._3()) {
//					ArrayList<Tuple2<String,String>> path = new ArrayList<Tuple2<String,String>>();
					if(p.getValue()._3()!=null && !p.getValue()._3().isEmpty()) {
						ArrayList<Tuple2<String,String>> path = new ArrayList<Tuple2<String,String>>(p.getValue()._3());
//						path=p.getValue()._3();
//						System.out.println("\n\n\t\tpath prima: "+path);

						path.add(new Tuple2<String,String>(p.getKey()._1(),neigh));
						Tuple3<String, String, ArrayList<String>> key = 
								new Tuple3<String, String, ArrayList<String>> 
									(neigh, p.getKey()._2(),null);
							Tuple3<Integer, String, ArrayList<Tuple2<String,String>>> value = 
								new Tuple3<Integer, String, ArrayList<Tuple2<String,String>>>
									(p.getValue()._1()+1,"GREY",path);
							Protein n = new Protein(key,value);	
							output.add(n);
					}
					else {
//						System.out.println("\n\n\t\tpath prima: VUOTO");
						ArrayList<Tuple2<String,String>> path = new ArrayList<Tuple2<String,String>>();
						path.add(new Tuple2<String,String>(p.getKey()._1(),neigh));
						Tuple3<String, String, ArrayList<String>> key = 
								new Tuple3<String, String, ArrayList<String>> 
									(neigh, p.getKey()._2(),null);
							Tuple3<Integer, String, ArrayList<Tuple2<String,String>>> value = 
								new Tuple3<Integer, String, ArrayList<Tuple2<String,String>>>
									(p.getValue()._1()+1,"GREY",path);
							Protein n = new Protein(key,value);	
							output.add(n);
					}
				}
				}
			Tuple3<String,String,ArrayList<String>> blackKey = 
					new Tuple3<String,String,ArrayList<String>>(p.getKey()._1(),p.getKey()._2(),p.getKey()._3());		
			Tuple3<Integer, String, ArrayList<Tuple2<String,String>>> blackValue = 
						new Tuple3<Integer, String, ArrayList<Tuple2<String,String>>>(p.getValue()._1(),"BLACK",p.getValue()._3());
			Protein black = new Protein(blackKey,blackValue);
			output.add(black);
			}
			
		return output.iterator();
	}

	public static void printProtein(Protein p) {
		Tuple3<String, String, ArrayList<String>> Key = p.getKey();
		Tuple3<Integer, String, ArrayList<Tuple2<String, String>>> Value = p.getValue();
		System.out.println("\t"+Key._1()+"\t"+Key._2()+"\t"+Key._3()+
				"\t"+Value._1()+"\t"+Value._2()+"\t"+Value._3());
	}

}
