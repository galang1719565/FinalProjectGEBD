package JcomeJulian;

import java.util.Comparator;

import scala.Serializable;
import scala.Tuple2;

public class EdgesComparator implements Serializable, Comparator<Tuple2<Tuple2<String, String>,Float>> {

	public int compare(Tuple2<Tuple2<String, String>, Float> o1, Tuple2<Tuple2<String, String>, Float> o2) {
			
		if(o1!=null && o2!=null) {
			
			if (o1._2 > o2._2) {
				return 1;
			} else if (o1._2 == o2._2) {
				return 0;
			} else {
				return -1;
			}
		}
		return 0;	
	}

}
