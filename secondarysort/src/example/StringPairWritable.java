package example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class StringPairWritable implements WritableComparable<StringPairWritable> {

  String left;
  String right;

  /**
   * Empty constructor - required for serialization.
   */ 
  public StringPairWritable() {

  }

  /**
   * Constructor with two String objects provided as input.
   */ 
  public StringPairWritable(String left, String right) {
    this.left = left;
    this.right = right;    
  }

  /**
   * Serializes the fields of this object to out.
   */
  public void write(DataOutput out) throws IOException {
	  //output using UTF
	  out.writeUTF(left);
	  out.writeUTF(right);
  }

  /**
   * Deserializes the fields of this object from in.
   */
  public void readFields(DataInput in) throws IOException {
	  // use UTF to read file
	  left = in.readUTF();
	  right = in.readUTF();
    
  }

  /**
   * Compares this object to another StringPairWritable object by
   * comparing the left strings first. If the left strings are equal,
   * then the right strings are compared.
   */
  public int compareTo(StringPairWritable other) {
	  
    int ret = left.compareTo(other.left);
    // if left same check right 
    if(ret==0){
    	ret = right.compareTo(other.right); 
    }
    
    return ret;
  }
  
  public String getLeft(){ // accessor for left 
	  return this.left;
  }
  
  public String getRight(){ // accessor for right 
	  return this.right;
  }

  public void  setLeft(String left) {
	  this.left = left;
  }
  
  public void setRight(String right) {
	  this.right = right;
  }
  /**
   * A custom method that returns the two strings in the 
   * StringPairWritable object inside parentheses and separated by
   * a comma. For example: "(left,right)".
   */
  public String toString() {
     return "(" + left + "," + right + ")";
  }

  @Override
public boolean equals(Object obj) {
	if (this == obj)
		return true;
	if (obj == null)
		return false;
	if (getClass() != obj.getClass())
		return false;
	StringPairWritable other = (StringPairWritable) obj;
	if (left == null) {
		if (other.left != null)
			return false;
	} else if (!left.equals(other.left))
		return false;
	if (right == null) {
		if (other.right != null)
			return false;
	} else if (!right.equals(other.right))
		return false;
	return true;
}

  @Override
public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((left == null) ? 0 : left.hashCode());
	result = prime * result + ((right == null) ? 0 : right.hashCode());
	return result;
}
}
