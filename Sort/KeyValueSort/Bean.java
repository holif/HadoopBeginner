import java.io.DataInput;  
import java.io.DataOutput;  
import java.io.IOException;  

import org.apache.hadoop.io.WritableComparable;  

public class Bean implements WritableComparable<Bean> {  
  
    private String carName;  
    private long sum;  
  
    public Bean() {		
    }  
    public Bean(String carName, long sum) { 
        this.carName = carName;  
        this.sum = sum;  
    }  
    @Override  
    public void write(DataOutput out) throws IOException {  
        out.writeUTF(carName);  
        out.writeLong(sum);  
    }  
    @Override  
    public void readFields(DataInput in) throws IOException {  
        this.carName = in.readUTF();  
        this.sum = in.readLong();  
    }  
	
	public String getCarName() {
		return carName;
	}
	public void setCarName(String carName) {
		this.carName = carName;
	}
	public long getSum() {
		return sum;
	}
	public void setSum(long sum) {
		this.sum = sum;
	}  
  
    @Override  
    public String toString() {  
        return "" + sum;  
    }  
    @Override  
    public int compareTo(Bean o) {  
        return this.sum > o.sum ? -1 : 1;  
    }  
}  