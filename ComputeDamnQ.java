package JcomeJulian;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class ComputeDamnQ implements Function<
	Tuple2<Tuple2<Integer,List<List<Tuple2<String,String>>>>,ArrayList<Tuple2<String,String>>>, //input
	Tuple2<List<List<Tuple2<String,String>>>,Float> //output
	> {

	@Override
	public Tuple2<List<List<Tuple2<String,String>>>,Float> 
		call(Tuple2<Tuple2<Integer,List<List<Tuple2<String,String>>>>,ArrayList<Tuple2<String,String>>> line) 
				throws Exception {
		//definisco gli oggetti in input:
		List<List<Tuple2<String,String>>> edgesComp = line._1()._2; //Lista degli archi divisi in liste di componenti
		ArrayList<Tuple2<String,String>> edges = line._2(); //Lista di tutti gli archi
		
		
		Integer boh=0;
		Integer denom = (int) edges.size();
		Integer numeroComp = edgesComp.size();
		Float[][] f = new Float[numeroComp][numeroComp];
        for(int i=0;i<numeroComp;i++) {
        	List<Tuple2<String,String>> line_i = edgesComp.get(i);
        	f[i][i] = (float) line_i.size()/denom;
        	for(int j=0;j<i;j++) {
        			List<Tuple2<String,String>> line_j = edgesComp.get(j);
                	boh = damnEdges(line_i,line_j,edges);
                	f[i][j]=(float) (boh/denom);
                   	f[j][i]=f[i][j];
        	}
        }
        
        
        
/*        System.out.println("\n\n\tComponente: "+edgesComp+"\n\tf[][] : ");
        System.out.println("\tdenom: "+denom+"\tnumeroComp: "+numeroComp);
		System.out.println("\tdamnEdges : "+boh);
		  
        for(int i=0;i<numeroComp;i++) {
        	for(int j=0;j<numeroComp;j++) {
//        			Float[] f_i = f[i];
        			System.out.println("\tf["+(i+1)+"]["+(j+1)+"] = "+f[i][j]);
//        			System.out.println("\t"+f_i);
        			        			
        	}
        }
  */      
        
        
		Float damnQ=damnQ(numeroComp,f);
		return new Tuple2<List<List<Tuple2<String,String>>>,Float>(edgesComp,damnQ);
	}

	
	
	
	
	private Integer damnEdges(List<Tuple2<String,String>> line_i, List<Tuple2<String,String>> line_j,
			ArrayList<Tuple2<String,String>> edges) {
			edges.removeAll(line_j);
			edges.removeAll(line_i);
			Integer count=0;
			for(Tuple2<String,String> e : edges) {
				if(Link(e,line_j,line_i)||Link(e,line_i,line_j)) count++;
			}
		
		
		return count;
	}

	
	private static boolean Link(Tuple2<String,String> e, List<Tuple2<String,String>> line_j,
			List<Tuple2<String,String>> line_i) {
  		
		String e1,e2;//,a,b,c,d;
		e1=e._1();e2=e._2();
		boolean results = false;
		for(Tuple2<String,String> e_i:line_i) {
			if(e_i._1().equals(e1)) {
				for(Tuple2<String,String> e_j : line_j)
				if(e_j._1().equals(e2)||e_j._2().equals(e2)) results=true;}
			if(e_i._2().equals(e1)) {
				for(Tuple2<String,String> e_j : line_j)
				if(e_j._1().equals(e2)||e_j._2().equals(e2)) results=true;}
		}

		return results;
	}
	
	
	
	public static float damnQ(Integer k, Float[][] f) {
		Float Q = (float) 0;
		for(int i=0;i<k;i++){
			Float meno = (float) 0;
			for(int j=0;j<k;j++){meno+=f[i][j];}
			Q += f[i][i]-(meno*meno);
		}
		return Q;
	}
	
	
	
	
	
	
}
