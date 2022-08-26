package csc369;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;

public class HostUrlCountPair
        implements Writable, WritableComparable<HostUrlCountPair> {

    private final Text country = new Text();
    private final Text url = new Text();
    private final IntWritable sum = new IntWritable();

    public HostUrlCountPair() {
    }

    public HostUrlCountPair(String country, String url, int sum) {
        this.country.set(country);
        this.url.set(url);
        this.sum.set(sum);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.country.write(out);
        this.url.write(out);
        this.sum.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.country.readFields(in);
        this.url.readFields(in);
        this.sum.readFields(in);
    }

    @Override
    public int compareTo(HostUrlCountPair pair) {
        if (this.country.compareTo(pair.getCountry()) == 0) {
            return -1 * sum.compareTo(pair.sum);
        }
        return this.country.compareTo(pair.getCountry());
    }

    public Text getCountryUrl() {
        return new Text(this.country.toString() + " " + this.url.toString());
    }

    public Text getCountry() {
        return this.country;
    }

    public Text getUrl() {
        return this.url;
    }

    public IntWritable getSum() {
        return this.sum;
    }

}
