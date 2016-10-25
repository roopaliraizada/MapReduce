package hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
@Description(
		name = "formatrecord",
		value = "_FUNC_(str) - Converts a string to uppercase",
		extended = "Example:\n" +
		"  > SELECT formatrecord(id, anme, details) FROM o a;\n" +
		"  STEPHEN KING"
		)
public class FormatRecord extends UDF {
	  
	  /*public Text evaluate(Text name) {
	    if(name == null) return null;
	    return new Text(name.toString().toUpperCase());
	  }*/
	public Text evaluate(Text id, Text name, Text details){
		return new Text (name +" has id = " + id + " and works as a " + details);
	}
	}
