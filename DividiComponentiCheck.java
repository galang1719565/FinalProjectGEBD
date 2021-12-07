package JcomeJulian;

import java.util.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

public class DividiComponentiCheck implements FlatMapFunction
<ArrayList<Tuple2<String,String>>,
ArrayList<Tuple2<String,String>> > 
{

	@Override
	public Iterator<ArrayList<Tuple2<String, String>>> call(ArrayList<Tuple2<String, String>> allEdges)
			throws Exception {

/*		System.out.println("\n\t ______________________________________________________________\n"
				          + "\t|                                                              |\n"
				          + "\t|  Funzione ***DividiComponenti implements FlatMapFunction***  |\n"
                          + "\t|______________________________________________________________|\n");
	*/	
		ArrayList<ArrayList<Tuple2<String,String>>> results = new ArrayList<ArrayList<Tuple2<String,String>>> ();
		ArrayList<Tuple2<String,String>> AllEdges = new ArrayList<Tuple2<String,String>>(allEdges);

		ArrayList<ArrayList<Tuple2<String,String>>> listona = new ArrayList<ArrayList<Tuple2<String,String>>>();
		ArrayList<Tuple2<String,String>> appoggio = new ArrayList<Tuple2<String,String>> ();
	
		Tuple2<String,String> first = AllEdges.get(0);
		appoggio.add(first);
		listona.add(appoggio);
		AllEdges.remove(first);
		boolean added = false;
		
		for(Tuple2<String,String> e:AllEdges) {
			added=false;
			for(ArrayList<Tuple2<String,String>> l : listona) {
				if(check(e,l)) {
					l.add(e);   added=true; break;
					}
			}
			if(!added) {
				ArrayList<Tuple2<String,String>> nuova = new ArrayList<Tuple2<String,String>> ();
				nuova.add(e);
				listona.add(nuova);
			}
			
		}

		
		for(ArrayList<Tuple2<String,String>> l : listona) {
			results.add(l);
		}
		
		results=DoubleCheck(results);
		
		return results.iterator();
	}

	private ArrayList<ArrayList<Tuple2<String, String>>> DoubleCheck(
			ArrayList<ArrayList<Tuple2<String, String>>> listona) {
		
		ArrayList<ArrayList<Tuple2<String,String>>> results = new ArrayList<ArrayList<Tuple2<String,String>>>();

		for(ArrayList<Tuple2<String, String>> output2:listona) {
		ArrayList<Tuple2<String,String>> output = new ArrayList<Tuple2<String,String>>(output2);
		
		for(ArrayList<Tuple2<String, String>> comp:listona) {
				ArrayList<Tuple2<String,String>> comp2 = new ArrayList<Tuple2<String,String>>(comp);
					
				if(!output.equals(comp2)) {
				boolean flag=false;
/*				System.out.println("\n\t _____________________________________________________\n"
				          + "\t|                                                     |\n"
				          + "\t|  Funzione ***DoubleCheck2 implements ReduceByKey*** |\n"
		                  + "\t|_____________________________________________________|\n");

				System.out.println("\n\n________________________________________________________\n");
				System.out.println("\tComponente1: "+output+"\tNOT EQUAL\tComponente2: "+comp2);
				System.out.println("________________________________________________________\n\n");
*/				
				
				while(!flag&&!comp2.isEmpty())	{
					Tuple2<String,String> e = new Tuple2<String,String>(null, null);
					e = comp2.get(0);
//					System.out.println("check(e,comp1) = check( ("+e._1+", "+e._2+"), "+output+"  )");
					if(!output.isEmpty()&&check(e,output)) {
						flag = true;
//						System.out.println("CHEEEEEEEECK");
					}
					comp2.remove(e);
					}

				if(flag) output.addAll(comp);}

				results.add(output);
/*				System.out.println("\n###"
						+ "\n### results.add(output)\t\toutput="+output2
						+ "\n### \t\t\t\t\tcomp2="+comp
						+ "\n###\n");
	*/			
				
			}
			
		}
		return results;
	}

	private boolean check(Tuple2<String, String> e, ArrayList<Tuple2<String, String>> list) {
		boolean result = false;
		ArrayList<Tuple2<String,String>> list1 = new ArrayList<Tuple2<String,String>>(list);
		while(result!=true&&!list1.isEmpty()) {
			Tuple2<String, String> arco = list1.get(0);
			if(e._1.equals(arco._1)||e._1.equals(arco._2)||e._2.equals(arco._2)||e._2.equals(arco._1)) result=true;
			list1.remove(arco);
		}
		return result;
	}

}
