
import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EndomondoWritable implements WritableComparable {

   DoubleWritable longitude, altitude, latitude, heart_rate, distance;
   LongWritable  timestamp, userId, id, duration;
   Text sport;
   Text gender;
   
   public EndomondoWritable(){
       longitude = new DoubleWritable();
       latitude = new DoubleWritable();
       altitude = new DoubleWritable();
       sport = new Text();
       id = new LongWritable();
       gender = new Text();
       userId = new LongWritable();
       heart_rate = new DoubleWritable();
       timestamp = new LongWritable();
       distance = new DoubleWritable();
       duration = new LongWritable();
   };

    public EndomondoWritable(double longitude, double altitude, double latitude, String sport, long id, String gender, long userId, double heart_rate, long timestamp, double distance, long duration) {
        this.longitude.set(longitude);
        this.altitude.set(altitude);
        this.latitude.set(latitude);
        this.sport.set(sport);
        this.gender.set(gender);
        this.id.set(id);
        this.userId.set(userId);
        this.heart_rate.set(heart_rate);
        this.timestamp.set(timestamp);
        this.distance.set(distance);
        this.duration.set(duration);
    }

    public String getGender() {
        return gender.toString();
    }

    public void setGender(String gender) {
        this.gender.set(gender);
    }

    public double getLongitude() {
        return longitude.get();
    }

    public void setLongitude(double longitude) {
        this.longitude.set(longitude);
    }

    public double getAltitude() {
        return altitude.get();
    }

    public void setAltitude(double altitude) {
        this.altitude.set(altitude);
    }

    public double getLatitude() { return latitude.get(); }

    public void setLatitude(double latitude) { this.latitude.set(latitude); }

    public String getSport() {
        return sport.toString();
    }

    public void setSport(String sport) {
        this.sport.set(sport);
    }

    public long getId() {
        return id.get();
    }

    public void setId(long id) {
        this.id.set(id);
    }

    public long getUserId() {
        return userId.get();
    }

    public void setUserId(long userId) {
        this.userId.set(userId);
    }

    public double getHeart_rate() { return heart_rate.get(); }

    public void setHeart_rate(double heart_rate) { this.heart_rate.set(heart_rate); }

    public long getDuration() { return duration.get(); }

    public void setDuration(long duration) { this.duration.set(duration); }

    public long getTimestamp() { return timestamp.get(); }

    public void setTimestamp(long timestamp) { this.timestamp.set(timestamp); }

    public void setDistance(double distance){this.distance.set(distance);}

    public  double getDistance(){return this.distance.get();}
    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
    public void decode(String line){
        String[] data = line.split(",", -1);
        data[0] = data[0].replaceAll("[^0-9]+", "");
        setId(Long.parseLong(data[0]));
        setUserId(Long.parseLong(data[1]));
        setGender(data[2]);
        setSport(data[3]);
        setHeart_rate(Double.parseDouble(data[4]));
        setTimestamp(Long.parseLong(data[5]));
        setDistance(Double.parseDouble(data[6]));
        setLongitude(Double.parseDouble(data[7]));
        setLatitude(Double.parseDouble(data[8]));
        setAltitude(Double.parseDouble(data[9]));
        setDuration(Long.parseLong(data[10]));
    }
    public String encode(){
        String endo = String.valueOf(getId())+","+String.valueOf(getUserId())+","+getGender()+","+getSport()+","+String.valueOf(getHeart_rate())+","+String.valueOf(getTimestamp())
                +","+String.valueOf(getDistance())+","+String.valueOf(getLongitude())+","+String.valueOf(getLatitude())+","+String.valueOf(getAltitude())+","+String.valueOf(getDuration());
        return endo;
    }
}
