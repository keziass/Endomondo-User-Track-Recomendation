import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import java.lang.Math;
import java.io.*;
import java.sql.Timestamp;
import java.util.*;

public class Analysis {
    static EndomondoWritable user;
    static Endomondo userfinal;
    static List<ArrayList<Long>> namesAndNumbers = new ArrayList<ArrayList<Long>>();
    static Long list[];

    enum Counters{
        OUTPUT_RECORDS,
        INPUT_RECORDSz
    }


    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            EndomondoWritable endo = new EndomondoWritable();
            endo.decode(line);
            if (endo.getSport().equals(user.getSport()) && endo.getUserId() == user.getUserId()) {
                Timestamp timestamp1 = new Timestamp(endo.getTimestamp());
                Timestamp timestamp2 = new Timestamp(user.getTimestamp());
                if(timestamp1.after(timestamp2)){
                    if (user.getDistance() <  endo.getDistance() || user.getDuration() < endo.getDuration()) {
                        user=endo;
                    }
                }
            }
            else if (endo.getSport().equals(user.getSport()) && endo.getGender().equals(user.getGender())){
                String coord = endo.encode();
                context.write(new Text(Long.toString(endo.getId())), new Text(coord));
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        NullWritable out = NullWritable.get();

        public void reduce(Text key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {
            Double distance = 0.0, distance2=0.0;
            EndomondoWritable endo = new EndomondoWritable();
            for (Text val : values){
                endo.decode(val.toString());
                distance = distanceTo(user.getLatitude(),user.getLongitude(),endo.getLatitude(),endo.getLongitude());

                if(endo.getDistance() >= user.getDistance()) {
                    if(distance < 100){
                        namesAndNumbers.add(new ArrayList<Long>(Arrays.asList(endo.getId(), distance.longValue())));
                        context.write(new Text(key), new DoubleWritable(distance));
                        context.getCounter(Counters.OUTPUT_RECORDS).increment(1);
                    }
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        user = new EndomondoWritable();
        userfinal = new Endomondo();
        user.setUserId(10921915);
        user.setSport("bike");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Filter Sport");

        job.setJarByClass(EndomondoWritable.class);
        job.setMapperClass(cari.TokenizerMapper.class);
        job.setReducerClass(cari.IntSumReducer.class);

        // Untuk Output Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // Untuk Output Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);


        FileInputFormat.addInputPath(job, new Path("Input/endomondo2.csv"));
        FileOutputFormat.setOutputPath(job, new Path("OutputProyek/filter"));

        job.waitForCompletion(true);

        Collections.sort(namesAndNumbers, new Comparator<ArrayList<Long>>() {
            @Override
            public int compare(ArrayList<Long> o1, ArrayList<Long> o2) {
                return o1.get(1).compareTo(o2.get(1));
            }
        });

        System.out.println(namesAndNumbers);
        if(namesAndNumbers.size()<5){
            list = new Long[]{};
            for(int i=0;i<namesAndNumbers.size();i++){
                list[i] = namesAndNumbers.get(i).get(0);
            }
        }
        else{
            list= new Long[]{namesAndNumbers.get(0).get(0), namesAndNumbers.get(1).get(0), namesAndNumbers.get(2).get(0), namesAndNumbers.get(3).get(0), namesAndNumbers.get(4).get(0)};

        }
        JSONParser parser = new JSONParser();

        ArrayList<Endomondo> data = new ArrayList<>();
        JSONObject obj;
        String line = null;

        Reader reader = new FileReader("C:\\Users\\ASUS\\IdeaProjects\\Proyek\\input\\endomondoHR_proper.json");
        BufferedReader bufferedReader = new BufferedReader(reader);
        while((line = bufferedReader.readLine()) != null) {
            obj = (JSONObject) new JSONParser().parse(line);
            if(Arrays.asList(list).contains(obj.get("id"))||obj.get("id").equals(user.getId())){
                JSONArray longitude = (JSONArray) obj.get("longitude");
                JSONArray altitude = (JSONArray)obj.get("altitude");
                JSONArray latitude = (JSONArray)obj.get("latitude");
                JSONArray heart_rate = (JSONArray)obj.get("heart_rate");
                JSONArray timestamp = (JSONArray)obj.get("timestamp");
                String sport = (String)obj.get("sport");
                String gender = (String)obj.get("gender");
                Long id = (Long)obj.get("id");
                Long userid = (Long)obj.get("userId");
                Double[] longt,lat,alt;
                Double distance=0.0;
                Double x1=0.0,y1=0.0,z1=0.0,x2=0.0,y2=0.0,z2=0.0;
                Long[] heart;
                heart = new Long[longitude.size()];
                longt = new Double[longitude.size()];
                lat = new Double[longitude.size()];
                alt = new Double[longitude.size()];
                for(int i=0;i<longitude.size();i++){
                    if(i>0){
                        x1=(double)longitude.get(i);
                        x2=(double)longitude.get(i-1);
                        y1=(double)latitude.get(i);
                        y2=(double)latitude.get(i-1);

                    }
                    longt[i]=(double)longitude.get(i);
                    lat[i]=(double)latitude.get(i);
                    alt[i]=(double)altitude.get(i);
                    heart[i]=(long)heart_rate.get(i);
                    distance = distance + distanceTo(y1,x1,y2,x2);
                }
                long time = (long)timestamp.get(timestamp.size()-1) - (long)timestamp.get(0);
                Endomondo end = new Endomondo(longt,alt,lat,heart,distance,sport,id,time,gender,userid);
                System.out.println(end.getLat());
                System.out.println(end.getLon());
                end.setLocation(distanceTo(user.getLatitude(),user.getLongitude(),end.getLat(),end.getLon()));
                if(obj.get("id").equals(user.getId())){
                    userfinal = end;
                }
                else{
                    data.add(end);
                }
            }
            if(data.size()==5){
                break;
            }
        }
        bufferedReader.close();
        for(int i =0; i<data.size(); i++){
            System.out.println("Data Top 5 " + i + "\n");
            data.get(i).print();

        }
        System.out.println("\nHASIL : " + data.size());
        FileWriter csvWriter = new FileWriter("C:\\Users\\ASUS\\IdeaProjects\\Proyek\\input\\TOP5.csv");
        csvWriter.append("id");
        csvWriter.append(",");
        csvWriter.append("userId");
        csvWriter.append(",");
        csvWriter.append("Gender");
        csvWriter.append(",");
        csvWriter.append("Sport");
        csvWriter.append(",");
        csvWriter.append("avg_heart_rate");
        csvWriter.append(",");
        csvWriter.append("location");
        csvWriter.append(",");
        csvWriter.append("distance");
        csvWriter.append(",");
        csvWriter.append("duration");
        csvWriter.append("\n");

        for (int i=0; i<data.size(); i++){
            System.out.println("Data sorted ke - "+i+" - "+data.get(i).getId() + " -- " + data.get(i).getLocation());
            csvWriter.append(String.valueOf(data.get(i).getId()));
            csvWriter.append(",");
            csvWriter.append(String.valueOf(data.get(i).getUserId()));
            csvWriter.append(",");
            csvWriter.append(data.get(i).getGender());
            csvWriter.append(",");
            csvWriter.append(data.get(i).getSport());
            csvWriter.append(",");
            csvWriter.append(String.valueOf(data.get(i).getAvgHeart()));
            csvWriter.append(",");
            csvWriter.append(String.valueOf(data.get(i).getLocation()));
            csvWriter.append(",");
            csvWriter.append(String.valueOf(data.get(i).getDistance()));
            csvWriter.append(",");
            csvWriter.append(String.valueOf(data.get(i).getDuration()));
            csvWriter.append("\n");
        }
        csvWriter.flush();
        csvWriter.close();
        System.out.println("id : " +user.getId());
        System.out.println("gender user: " + user.getGender());
        System.out.println("duration user: " + user.getDuration());
        System.out.println("distance user: " + user.getDistance());
        System.out.println("heart user: " + user.getHeart_rate());


        System.out.println("\nTES GENDER\n");
        ArrayList<Endomondo> hasildistance = new ArrayList<>();
        double rdistance=0.0;

        for(int i=0; i<data.size(); i++){
            rdistance=Math.abs(data.get(i).getDistance()-user.getDistance());
            System.out.println("rdistance " + rdistance);
            if(rdistance<=100){
                hasildistance.add(data.get(i));
            }
        }

        System.out.println("\nTES DISTANCE\n");
        ArrayList<Endomondo> hasilduration = new ArrayList<>();
        long rduration;
        if(hasildistance.size()==0){
            System.out.println("\nDistance kosong");
            hasildistance=data;
        }
        for(int i=0; i<hasildistance.size(); i++){
            System.out.println("Distance " + hasildistance.size());
            rduration=Math.abs(hasildistance.get(i).getDuration()-user.getDuration());
            System.out.println("rduration " + rduration);
            System.out.println(rduration);

            if(rduration<=3600){
                hasilduration.add(hasildistance.get(i));
            }
        }

        System.out.println("\nTES DURATION\n");
        ArrayList<Endomondo> hasil = new ArrayList<>();
        double rheart=0.0;
        if(hasilduration.size()==0){
            System.out.println("\nDuration kosong");
            hasilduration=hasildistance;
        }

        for(int i=0; i<hasilduration.size(); i++){
            System.out.println("Duration " + hasilduration.size());
            rheart=Math.abs(hasilduration.get(i).getAvgHeart()-user.getHeart_rate());
            System.out.println("rheart " + rdistance);
            if(rheart<=20){
                hasil.add(hasilduration.get(i));
            }
        }

        System.out.println("\nTES HEART_RATE\n");
        if (hasil.size()==0){
            System.out.println("\nHeart kosong");
            hasil=hasilduration;
        }
        System.out.println("Heart " + hasil.size());
        for (int i = 0; i < hasil.size(); i++) {
            hasil.get(i).print();
        }

        System.out.println("Data User");
        userfinal.print();
    }

    private static double distanceTo(double lat1, double lon1, double lat2, double lon2) {
        if ((lat1 == lat2) && (lon1 == lon2)) {
            return 0;
        }
        else {
            double theta = lon1 - lon2;
            double dist = Math.sin(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2)) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(theta));
            dist = Math.acos(dist);
            dist = Math.toDegrees(dist);
            dist = dist * 60 * 1.1515;
            dist = dist * 1.609344;
            return (dist);
        }
    }
}