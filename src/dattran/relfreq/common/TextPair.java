package dattran.relfreq.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair> {

	private String left;
	private String right;
	
	public TextPair(String left, String right) {
		super();
		this.left = left;
		this.right = right;
	}

	public TextPair() {
		super();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		left = in.readUTF();
		right = in.readUTF();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(left);
		out.writeUTF(right);
	}

	@Override
	public int compareTo(TextPair o) {
		int compareValue = compareIntString(left, o.left);
		if (compareValue == 0) {
			return compareIntString(right, o.right);
		}else{
			return compareValue;
		}
	}
	
	public int compareIntString(String r, String l) {
		try {
			int rValue = Integer.parseInt(r);
			int lValue = Integer.parseInt(l);
			if (rValue < lValue) {
				return -1;
			} else if (rValue > lValue) {
				return 1;
			} else return 0;
		} catch (Exception e) {
			return r.compareTo(l);
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s, %s", left,right);
	}

	public String getLeft() {
		return left;
	}

	public void setLeft(String left) {
		this.left = left;
	}

	public String getRight() {
		return right;
	}

	public void setRight(String right) {
		this.right = right;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TextPair) {
			TextPair textPairObj = (TextPair) obj;
			boolean equality = compareIntString(left, textPairObj.left) == 0 &&
					compareIntString(right, textPairObj.right) == 0;
			return equality;
		} else {
			return false;
		}
	}
	
	@Override
	public int hashCode() {
		return left.hashCode() * 1000 + right.hashCode();
	};
}
