package JcomeJulian;

import java.text.DecimalFormat;
import java.util.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

public class JcomeJava {

	
	
	/********************METODI********************/

	
	private static JavaRDD<Protein> createInput(JavaPairRDD<String, String> Edges) {
	
		/*
		 * Per ogni arco (a,b) 
		 * Estrapolo le coppie <NodeId,Neighbor>
		 * 					   <a,b> e <b,a>
		 */
		JavaPairRDD<String,ArrayList<String>> input1 = Edges.mapToPair(e -> {
	    	ArrayList<String> boh = new ArrayList<String>(); boh.add(e._2);
			return new Tuple2<String,ArrayList<String>>(e._1,boh);}	);
	    JavaPairRDD<String,ArrayList<String>> input2 = Edges.mapToPair(e -> {
	    	ArrayList<String> boh = new ArrayList<String>(); boh.add(e._1);
			return new Tuple2<String,ArrayList<String>>(e._2,boh);}	);
	    input1 = input1.union(input2);
		
	    // Preparo la struttura dei neighbors come lista
		JavaPairRDD<String,ArrayList<String>> input3 = input1.mapToPair(pair -> {
			String nodeId = pair._1;		ArrayList<String> neigh = pair._2;
			Tuple2<String,ArrayList<String>> result = new Tuple2<String,ArrayList<String>>(nodeId,neigh);
			return result;});
		
		/*
		 * Costruisco la matrice n x n
		 */
		JavaRDD<String> Nodes = Edges.keys().union(Edges.values()).distinct();
    	JavaPairRDD<String, Tuple2<String, ArrayList<String>>> NNN = Nodes.cartesian(input3); 
		//Separo <NodeId,Root> e <Neighbors>
    	JavaPairRDD<Tuple2<String, String>, ArrayList<String>> NNN2 = NNN.mapToPair(nnn -> {
			String n2=nnn._1;
			Tuple2<String, ArrayList<String>> nnn2=nnn._2;
			String n1 = nnn2._1;
			Tuple2<String, String> pair = new Tuple2<String, String>(n1,n2);
			ArrayList<String> neigh = nnn2._2;
			Tuple2<Tuple2<String, String>, ArrayList<String>> result = new Tuple2<Tuple2<String, String>, ArrayList<String>>(pair,neigh);
			return result;
		});
    	//Accorpo i neighbors
		NNN2 = NNN2.reduceByKey((n1,n2)-> {	ArrayList<String> neigh = new ArrayList<String>();
											if(!n1.isEmpty())neigh.addAll(n1); if(!n2.isEmpty())neigh.addAll(n2);
											return neigh; });
		
		/*
		 * Definisco la struttura di ogni riga della matrice n x n
		 */
		JavaRDD<Protein> input = NNN2.map(nnn -> {
			Tuple2<String, String> pair = nnn._1;
			Tuple3<String, String, ArrayList<String>> key = new Tuple3<String, String, ArrayList<String>>(pair._1, pair._2, nnn._2);
			Tuple3<Integer, String, ArrayList<Tuple2<String,String>>> value = 
					new Tuple3<Integer, String, ArrayList<Tuple2<String, String>>>(0, "WHITE", null);
			Protein p = new Protein(key, value);
			return p;
			});
		
		return input;
	}

	/*
	 * Struttura di una singola proteina:
	 * NodeId  Root  Neighbors | Distance | Color | Path
	 */
	public static void printProtein(Protein p) {
		Tuple3<String, String, ArrayList<String>> Key = p.getKey();
		Tuple3<Integer, String, ArrayList<Tuple2<String, String>>> Value = p.getValue();
		System.out.println("\t"+Key._1()+"\t"+Key._2()+"\t"+Key._3()+
				"\t"+Value._1()+"\t"+Value._2()+"\t"+Value._3());
	}

	private static JavaRDD<Protein> ForwardMR(JavaRDD<Protein> While) {
		
		//Eseguo il ForwardMR
		JavaRDD<Protein> Forward1 = While.flatMap(new ForwardMR());
		Forward1 = While.union(Forward1);
		//Prendo come chiave <NodeId,Root> per ottenere le nuove informazioni dall'output del ForwardMR
		JavaPairRDD<Tuple2<String,String>, Tuple4<ArrayList<String>,Integer,String,ArrayList<Tuple2<String,String>>>>
			Forward2 = Forward1.mapToPair(new Pairing());
		Forward2 = Forward2.reduceByKey(new getNeigh());
		//Ripristino la struttura della proteina
		Forward1=Forward2.map(new Reconstruct());
		return Forward1;
		
	}

	private static Tuple2<Tuple2<Integer, List<List<Tuple2<String, String>>>>, Tuple2<Tuple2<String, String>, Float>> BackwardMR
	(Integer step, JavaRDD<Protein> Results) {
	
		//Prendo tutti gli archi
		JavaRDD<ArrayList<Tuple2<String, String>>> archi0 = Results.map(p -> p.getValue()._3());
		archi0 = archi0.filter(a->a!=null);
		JavaRDD<Tuple2<String, String>> archiRDD = archi0.flatMap(l -> l.iterator());
		//Ordino gli archi
		archiRDD = archiRDD.map(a -> {
			int compare = a._1.compareTo(a._2);  
			if (compare > 0) return new Tuple2<String, String>(a._2,a._1);
			return a;
		});
		//Mi salvo gli archi che compongono la componente connessa
		List<Tuple2<String,String>> edgesComp = archiRDD.distinct().collect();
		List<List<Tuple2<String,String>>> componente = new ArrayList<List<Tuple2<String,String>>>();
		componente.add(edgesComp);
		/*
		 * BETWEENESS:
		 * BC(e) = sum(d_s,t(e))
		 * e edge, d_s,t(e) = number of shortest path from s to t
		 * 							passing through the edge e 
		 * 					_______________________________________
		 * 
		 * 					  number of shortest path from s to t
		 * 
		 */
		//Conto le occorrenze degli archi
		JavaPairRDD<Tuple2<String, String>,Integer> archiRDD2 = 
				archiRDD.mapToPair(x->new Tuple2<Tuple2<String, String>,Integer>(x,1));
		archiRDD2 = archiRDD2.reduceByKey((x,y)->x+y);
		JavaRDD<String> nodiComp = Results.map(p -> p.getKey()._1()).distinct();
		Integer numeroNodiComp = (int)nodiComp.count();
		//Calcola la Betweeness
		JavaPairRDD<Tuple2<String, String>,Float> Beetweeness = 
				archiRDD2.mapToPair(x->new Tuple2<Tuple2<String, String>,Float>(x._1,(float)x._2/(numeroNodiComp*(numeroNodiComp-1)))); 
		Tuple2<Tuple2<String, String>,Float> currentBC  = Beetweeness.max(new EdgesComparator());
		return new Tuple2<Tuple2<Integer,List<List<Tuple2<String,String>>>>,Tuple2<Tuple2<String,String>,Float>>
				(new Tuple2<Integer,List<List<Tuple2<String,String>>>>(step,componente),currentBC);
	}
	
	private static Tuple2<Tuple2<Integer, List<List<Tuple2<String,String>>>>, Tuple2<Tuple2<String, String>, Float>> ComputeBC(
			JavaRDD<Tuple2<Tuple2<Integer, List<List<Tuple2<String,String>>>>, Tuple2<Tuple2<String, String>, Float>>> BCRDD) {
		
		//Trovo l'arco con betweeness massima tra le componenti connesse del grafo
		JavaPairRDD<Tuple2<Integer, List<List<Tuple2<String,String>>>>, Tuple2<Tuple2<String, String>, Float>> BCRDD2 = 
				BCRDD.mapToPair(bc->new Tuple2<Tuple2<Integer, List<List<Tuple2<String,String>>>>, Tuple2<Tuple2<String, String>, Float>>(bc._1,bc._2));
		JavaRDD<Tuple2<Tuple2<String, String>, Float>> BCRDD3 = BCRDD2.map(p->p._2);
		Tuple2<Tuple2<String, String>,Float> currentBC  = BCRDD3.max(new EdgesComparator());
		Tuple2<Tuple2<Integer, List<List<Tuple2<String,String>>>>, Tuple2<Tuple2<String, String>, Float>> maxBC = 
				BCRDD2.filter(bc->bc._2.equals(currentBC)).first();
		return maxBC;
		
		}

	
	
	
	
	
	
	
	
	
	
	
	
	
	public static void main(String[] args) {

		Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("prova");       
        JavaSparkContext jsc = new JavaSparkContext(sc);

       /********************DATA LOADING & PREPROCESSING********************/

    
        JavaPairRDD<String,String> Edges, EdgesOriginal;
    	
    	//Dataset reale
		System.out.println("\n\n\n"+
				      "\t\t\t    @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
				    + "\t\t\t    @@@                                     @@\n"
					+ "\t\t\t    @@@      J   c o m e   J u l i a n      @@\n"
					+ "\t\t\t    @@@        J   c o m e   J a v a        @@\n"
					+ "\t\t\t    @@@                                     @@\n"
					+ "\t\t\t    @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
					+ "\t\t\t    @@@                                     @@\n"
					+ "\t\t\t    @@@           Dataset: R.Norv           @@\n"
					+ "\t\t\t    @@@                                     @@\n"
					+ "\t\t\t    @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
					+ "\n\n");
		JavaRDD<String> data = jsc.textFile("data/Rnorv20170205.txt"); //Prendo in input il file txt e ottengo gli archi
        data = data.filter(l -> !l.startsWith("ID"));
		Edges = data.mapToPair(x -> new Tuple2<String,String>(x.split("\t")[0],x.split("\t")[1]));
		
		//Elimino gli archi del tipo (a)->(a)
		Edges = Edges.filter(e->!e._1.equals(e._2()));
		EdgesOriginal = Edges;
		
		
		JavaRDD<String> Nodes = Edges.keys().union(Edges.values()).distinct();
		boolean sgrevato = Nodes.count()>10;	

		         

		
      	
      	
         /********************NEO4J********************/
			
			String uri = "bolt://localhost:7687";
			AuthToken token = AuthTokens.basic("neo4j", "daje");
			Driver driver = GraphDatabase.driver(uri, token);
			Session s = driver.session();
			System.out.println("\n\n\t\t\t\t\t _______________\n"
								 + "\t\t\t\t\t|               |\n"
								 + "\t\t\t\t\t|   N E O 4 J   |\n"
								 + "\t\t\t\t\t|_______________|\n");
			
			String cql = "match (n) detach delete n";
	   		s.run(cql);   	
	   		
	        List<String> nodi = Nodes.collect();
	        int count = 0;
			for(String n:nodi) {
		        String cql1 = "CREATE (n: protein {Id: '" + n + "',Color: 'WHITE'})";
		        s.run(cql1);count++;
	        }
	   		int count2 = 0;
			List<Tuple2<String, String>> interactions = Edges.collect();
			for (Tuple2<String, String> i : interactions) {
				String from = i._1; String to = i._2;
				cql = "MATCH (n1: protein {Id: '" + from + "' }),(n2: protein {Id: '" + to + "' }) CREATE (n1)-[:interaction]->(n2)";
				s.run(cql); count2++;
			}
			System.out.println("\tNumero nodi proteine creati: " + count + "\t\t\tNumero archi interazione aggiunti: " + count2 + "\n\n");
			
			//s.close();
         
			
         
         
         
         
         
         
         /**********************ALGORTIMO*********************/
		 System.out.println( "\n"
	         		+ "\t\t\t\t   ____________________________\n"
	 				+ "\t\t\t\t  |                            |\n"
	         		+ "\t\t\t\t  | Numero di step massimo = 5 |\n"
	 				+ "\t\t\t\t  |____________________________|\n"
	 				+ "\n"
	 				+ "");
	     Integer stepmax = 5;
  	
      	/*
		 * Lista su cui salverò < <Step,Lista<archi della componente>, <arco,betweeness> >
		 * ---> Per il calcolo di Q
		 */
		List<Tuple2<Tuple2<Integer,List<List<Tuple2<String,String>>>>,Tuple2<Tuple2<String,String>,Float>>> BC = 
				new ArrayList<Tuple2<Tuple2<Integer,List<List<Tuple2<String,String>>>>,Tuple2<Tuple2<String,String>,Float>>>();

		
		
		
		//L'algoritmo esegue il medesimo codice fino ad esaurire tutti gli archi presenti
      	Integer step=0;
      	while(!Edges.isEmpty()&&step<stepmax) {
			step++;
			System.out.println("\n\n\n\n\n\n"
+"_______________________________________________________________________________________________________________________________________________\n\n"
								 + "\n\t\t  S t e p :   "+step+"  \n\n"
+"_______________________________________________________________________________________________________________________________________________\n"
								 + "\n\n\n\n\n");
			
			/*
			 * Prendo gli archi e definisco le componenti
			 */
			
			JavaRDD<ArrayList<Tuple2<String,String>>> AllEdges = Edges.map(e->{
				ArrayList<Tuple2<String,String>> l = new ArrayList<Tuple2<String,String>>();
				l.add(e);
				return l;
			});
			JavaPairRDD<Integer,ArrayList<Tuple2<String,String>>> Edges3 = AllEdges.mapToPair(
					e->new Tuple2<Integer,ArrayList<Tuple2<String,String>>>(1,e) );
			Edges3 = Edges3.reduceByKey((l1,l2)->{l1.addAll(l2); return l1;});
			AllEdges = Edges3.map(e->e._2);
			
			JavaRDD<ArrayList<Tuple2<String, String>>> EdgesComp = AllEdges.flatMap(new DividiComponentiCheck());
			
			EdgesComp=EdgesComp.distinct().map(new Check()).distinct();
			
			//Elimino le componenti composte da pochi archi
			if(sgrevato)EdgesComp = EdgesComp.filter(comp->comp.size()>=10);
			
			Integer NumeroComponent = 0;
			NumeroComponent = EdgesComp.collect().size();
			
			/*
			 * Lista su cui salverò < <Step,Lista<archi della componente>, <arco,betweeness> >
			 * ---> Per calcolare la betweeness massima e trovare l'arco da eliminare
			 */
			List<Tuple2<Tuple2<Integer,List<List<Tuple2<String,String>>>>,Tuple2<Tuple2<String,String>,Float>>> BC_k = 
				new ArrayList<Tuple2<Tuple2<Integer,List<List<Tuple2<String,String>>>>,Tuple2<Tuple2<String,String>,Float>>>();//(bc1,bc2);
				
			Integer num_comp = 0;
			
			for(ArrayList<Tuple2<String,String>> comp : EdgesComp.collect()) {
				num_comp++;
				JavaPairRDD<String,String> Componente = jsc.parallelizePairs(comp);
				/*
				 * Prendo gli archi e costruisco il file di input n x n
				 */
				JavaRDD<Protein> input = createInput(Componente);
				JavaRDD<Protein> Start = input.map(p -> {
					if(p.getKey()._1().equals(p.getKey()._2()))
						p.setValue(new Tuple3<Integer, String, ArrayList<Tuple2<String,String>>>(0, "GREY", null));	
					return p;});
				Integer remaining = 1; Integer all = (int) Start.count();
				System.out.println("\n\t\tComponente  "+num_comp+" / "+NumeroComponent);
				System.out.println("\t\t\tProgress:");
				Integer black; float progress;
				while(remaining!=0) {
					Start = ForwardMR(Start);
					remaining = (int) Start.filter(p->!p.getValue()._2().equals("BLACK")).count();
					DecimalFormat df = new DecimalFormat("##.##");
					black = (int) Start.filter(p->p.getValue()._2().equals("BLACK")).count();
					progress = (float) 100*black/all;
					System.out.println("\t\t\t---> "+df.format(progress)+" %");
				}
				//Calcolo la betweeness della componente connessa appena esplorata
				BC_k.add(BackwardMR(step,Start.filter(p->p.getValue()._2().equals("BLACK"))));
			}//Fine ciclo for(componenti)	
		
			JavaRDD<Tuple2<Tuple2<Integer, List<List<Tuple2<String,String>>>>, Tuple2<Tuple2<String, String>, Float>>> 
				BCRDD = jsc.parallelize(BC_k);
			Tuple2<Tuple2<Integer, List<List<Tuple2<String,String>>>>, Tuple2<Tuple2<String, String>, Float>>
				maxBC = ComputeBC(BCRDD);
			//Elimino l'arco con betweeness massima
			Tuple2<String,String> arcociotto = maxBC._2()._1();
			Edges = Edges.filter(e->!e.equals(arcociotto));
			Tuple2<String,String> arcociotto2 = new Tuple2<String,String>(arcociotto._2,arcociotto._1);
			Edges = Edges.filter(e->!e.equals(arcociotto2));
			//Aggiungo alla lista <step,componente connessa>(,<arco,betweeness>)
			BC.addAll(BC_k);
		
			System.out.println("\n\n\tNUMERO DI COMPONENTI CONNESSE:\t"+num_comp+"\n");
			count = 0;
			for(Tuple2<Tuple2<Integer,List<List<Tuple2<String,String>>>>,Tuple2<Tuple2<String,String>,Float>> bc:BC_k){
				count++;
				Tuple2<Integer,List<List<Tuple2<String,String>>>> Componente = bc._1;
				Tuple2<Tuple2<String,String>,Float> beet = bc._2;
				System.out.println("\tComponente "+count);
				if(!sgrevato)System.out.println("\t\tArchi "+Componente._2);
				System.out.println("\t\tBeetweeness massima: "+beet);
			}
			System.out.println("\t---> Arco rimosso: "+arcociotto);
			
	    }//Fine ciclo while(!Edges.isEmpty)
	
      	System.out.println("\n\n\n\n\n\n"
      			+"_______________________________________________________________________________________________________________________________________________\n"
      											 + "\n\n\n\n\n");
      						
      	/*
		 * Passo al calcolo di Q
		 */
		//Considero le coppie <step, lista di archi di ogni componente>
		JavaPairRDD<Integer,List<List<Tuple2<String,String>>>> Q = 
				jsc.parallelize(BC).mapToPair(bc-> {
					Integer stepp = bc._1._1();
					List<List<Tuple2<String,String>>> comp = bc._1._2();
					return new Tuple2<Integer,List<List<Tuple2<String,String>>>>(stepp,comp);
				});
		//Accorpo le componenti connesse di uno stesso grafo
		Q = Q.reduceByKey((l1,l2)->{
			List<List<Tuple2<String, String>>> l = new ArrayList<List<Tuple2<String,String>>>();
			l.addAll(l1);l.addAll(l2);
			return l;
		});
		//Voglio un prodotto cartesiano dell'oggetto Q con la lista di tutti gli archi
		JavaPairRDD<Integer,ArrayList<Tuple2<String,String>>> EdgesList = EdgesOriginal.mapToPair(e->{
			ArrayList<Tuple2<String,String>> l = new ArrayList<Tuple2<String,String>>();
			l.add(e);
			return new Tuple2<Integer,ArrayList<Tuple2<String,String>>>(1,l);
			}
		);
		EdgesList = EdgesList.reduceByKey((l1,l2)->{l1.addAll(l2); return l1;});
		JavaRDD<ArrayList<Tuple2<String,String>>> EdgesList2 = EdgesList.map(e->e._2);
		//Oggetto 	<	<step, lista di componenti(archi generatori)>,	lista di tutti gli archi	>
		JavaPairRDD<Tuple2<Integer,List<List<Tuple2<String,String>>>>,ArrayList<Tuple2<String,String>>>
			Q2 = Q.cartesian(EdgesList2);
		//Calcolo di Q
		JavaRDD<Tuple2<List<List<Tuple2<String,String>>>,Float>> damnQ = Q2.map(new ComputeDamnQ());
		
		
		System.out.println("\n\n\n\n\t- Q -\n");
		System.out.println("\tvalue\t\t-n° componenti-\t\tgraph");
		for(Tuple2<List<List<Tuple2<String,String>>>,Float> q :damnQ.collect()) {
			DecimalFormat df = new DecimalFormat("#.###");
			Integer n_edg = q._1().size();
			if(!sgrevato)System.out.println("\t"+df.format(q._2)+"\t\t     - "+n_edg+" -     \t\t"+q._1());
			else System.out.println("\t"+df.format(q._2)+"\t\t     - "+n_edg+" -");
		}
		Tuple2<List<List<Tuple2<String,String>>>,Float> maxQ = damnQ.max(new QComparator());	
		List<List<Tuple2<String,String>>> maxQ_graph = maxQ._1();
		Integer best_num = maxQ_graph.size();
		
				
		System.out.println( "\n\n"
				+ "\t\t\t __________________________________________________________________________________________\n"
				+ "\t\t\t|                                                                                          |\n"
				+ "\t\t\t|                                                                                          |\n"
				+ "\t\t\t|                                                                                          |\n"
				+ "\t\t\t|                                      F i n a l l y:                                      |\n"
				+ "\t\t\t|                                                                                          |\n"
				+ "\t\t\t|                                                                                          |\n"
				+ "\t\t\t|                                                                                          |\n"
				+ "\t\t\t|                                                                                          |");
		if(best_num<10)System.out.println( 
				  "\t\t\t|                Il grafo ottimo risulta quello composto da "+best_num+" componenti.                  |");
		else if(best_num<100)System.out.println( 
				  "\t\t\t|               Il grafo ottimo risulta quello composto da "+best_num+" componenti.                  |");
		else System.out.println( 
				  "\t\t\t|               Il grafo ottimo risulta quello composto da "+best_num+" componenti.                 |");
		if(!sgrevato)System.out.println("\t\t\t|                                                                                          |\n"
				+ "\t\t\t|                                                                                          |\n"
				+ "\t\t\t|      Di seguito sono riportati gli archi che costituiscono le componenti connesse:       |\n"
				+ "\t\t\t|                                                                                          |");
		System.out.println("\t\t\t|                                                                                          |\n"
				+ "\t\t\t|__________________________________________________________________________________________|\n\n\n");
		count=0;
		System.out.println("\t\t\t\tQ = "+maxQ._2()+"\n");
		if(!sgrevato)System.out.println("\t\t\t\tComponente\t.\tArchi");
		else System.out.println("\t\t\t\tComponente\t.\tN° archi");
		for(List<Tuple2<String,String>> comp : maxQ_graph) {
			count++;
			if(!sgrevato)System.out.println("\t\t\t\t"+count+".\t"+comp);
			else System.out.println("\t\t\t\t"+count+".\t"+comp.size());
			}
		
		System.out.println("\n\n\n\n\n\n"
      			+"_______________________________________________________________________________________________________________________________________________\n"
      											 + "\n\n\n\n\n");
      		
			
			
			
			
		/********************NEO4J********************/
		//Scanner scanner = new Scanner(System.in);
	    
		System.out.println("\n\t\t\t ___________________\n"
						   + "\t\t\t|   _____________   |\n"
						   + "\t\t\t|  |             |  |\n"
						   + "\t\t\t|  |  N E O 4 J  |  |\n"
						   + "\t\t\t|  |_____________|  |\n"
						   + "\t\t\t|___________________|\n\n\n\n");
		cql = "match (n) detach delete n";
   		s.run(cql);   	
   		
		Integer num_comp = 0;
		JavaRDD<List<Tuple2<String, String>>> EdgesComp = jsc.parallelize(maxQ_graph);
		for(List<Tuple2<String,String>> comp : EdgesComp.collect()) {
			JavaPairRDD<String, String> boh = jsc.parallelizePairs(comp);
			JavaRDD<String> boh2 = boh.keys().union(boh.values()).distinct();
			num_comp++;
			JavaPairRDD<String,String> Componente = jsc.parallelizePairs(comp);
			JavaRDD<Protein> input = createInput(Componente);
			JavaRDD<Protein> Start = input.map(p -> {
				if(p.getKey()._1().equals(p.getKey()._2()))
					p.setValue(new Tuple3<Integer, String, ArrayList<Tuple2<String,String>>>(0, "GREY", null));	
				return p;});
			Integer remaining = (int) Start.filter(p->!p.getValue()._2().equals("BLACK")).count();
			step=0;
	   		String random2 = boh2.first();
	   		JavaRDD<Protein> Start2 = Start.filter(p->p.getKey()._2().equals(random2));
	   		JavaRDD<Tuple2<String,String>> boh32 = 
	   				Start2.map(p->new Tuple2<String,String>(p.getKey()._1(),p.getValue()._2()));
	        List<Tuple2<String,String>> nodi_comp2 = boh32.collect();
	        count = 0;
			for(Tuple2<String,String> n:nodi_comp2) {
		        String cql1 = "CREATE (n: protein {Id: '" + n._1 + "',Color: '"+n._2+"',Step: '"+step+"'})";
		        s.run(cql1);
	        }
			for (Tuple2<String, String> i : comp) {
				String from = i._1;
				String to = i._2;
				cql = "MATCH (n1: protein {Id: '" + from + "',Step: '"+step+"' }),(n2: protein {Id: '" + to + "' ,Step: '"+step+"'}) "
						+ "CREATE (n1)-[:interaction]->(n2)";
				s.run(cql);
			}
			System.out.println("\t\t\t  - Componente "+num_comp+" -");
			while(remaining!=0) {
				step++;	//n° of ForwardMR
				System.out.println("\t\t\t      [Step "+step+"]");
				Start = ForwardMR(Start);
			   		String random = boh2.first();
			   		Start = Start.filter(p->p.getKey()._2().equals(random));
			   		JavaRDD<Tuple2<String,String>> boh3 = 
			   				Start.map(p->new Tuple2<String,String>(p.getKey()._1(),p.getValue()._2()));
			        List<Tuple2<String,String>> nodi_comp = boh3.collect();
					for(Tuple2<String,String> n:nodi_comp) {
				        String cql1 = "CREATE (n: protein {Id: '" + n._1 + "',Color: '"+n._2+"',Step: '"+step+"'})";
						s.run(cql1);
			        }
					for (Tuple2<String, String> i : comp) {
						String from = i._1;
						String to = i._2;
						cql = "MATCH (n1: protein {Id: '" + from + "',Step: '"+step+"' }),(n2: protein {Id: '" + to + "' ,Step: '"+step+"'}) "
								+ "CREATE (n1)-[:interaction]->(n2)";
						s.run(cql);
					}
				remaining = (int) Start.filter(p->!p.getValue()._2().equals("BLACK")).count();
			}
			System.out.println("\t\t\t     [FINISHED]\n");
			
		}//Fine ciclo for(componenti)		
		
		s.close();		
			
			
		
		System.out.println("\n\n\n\n\n\n"
				+ "\t\t ___________________________________________________\n"
				+ "\t\t|                                                   |\n"
				+ "\t\t|   Per la visualizzazione del grafo:               |\n"
				+ "\t\t|     - Neo4J bloom per tutti gli step              |\n"
				+ "\t\t|           (Visualizzazione dei colori)            |\n"
				+ "\t\t|     - Neo4J browser:                              |\n" 
				+ "\t\t|           (Singolo step col comando sottostante)  |\n"
				+ "\t\t|            MATCH (j {Step:'1'}) RETURN j          |\n"
				+ "\t\t|___________________________________________________|\n\n\n\n\n\n");
			
			
		System.out.println("\n\n\n\n\n\n"
				+ "\t __________________________________________________________________________\n"
				+ "\t|                                                                          |\n"
				+ "\t|                    t  h  e                    e  n  d                    |\n"
				+ "\t|__________________________________________________________________________|\n\n\n\n\n\n");
			
	}
	
}
