import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by sniper on 16-1-14.
 */
public class TemWritable implements WritableComparable<TemWritable> {
    private String country;
    private String city;

    public TemWritable() {
        this.country = "";
        this.city = "";
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public TemWritable(String country, String city) {
        this.country = country;
        this.city = city;
    }

    public int compareTo(TemWritable o) {
        int rtn = (int)(this.country.hashCode() - o.country.hashCode());
//        if(rtn != 0)
            return rtn;
//        return (int)(this.city.hashCode() - o.city.hashCode());
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(country);
        dataOutput.writeUTF(city);
    }

    public void readFields(DataInput dataInput) throws IOException {
        country = dataInput.readUTF();
        city = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof TemWritable))
            return false;
        TemWritable other = (TemWritable) obj;
        return this.getCountry() == other.getCountry() && this.getCity() == other.getCity();
    }

    @Override
    public int hashCode() {
        return this.country.hashCode();
    }

    @Override
    public String toString() {
        return country +"\t" +city;
    }
}