import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.ArrayList;

public class JsonToCsv {

    enum Counters{
        OUTPUT_RECORDS,
        INPUT_RECORDS
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, NullWritable> {
        NullWritable out = NullWritable.get();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        }
    }

    public static boolean deleteDirectory(File dir) {
        if (dir.isDirectory()) {
            File[] children = dir.listFiles();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDirectory(children[i]);
                if (!success) {
                    return false;
                }
            }
        }

        // either file or an empty directory
        System.out.println("removing file or directory : " + dir.getName());
        return dir.delete();
    }

    public static void main(String[] args) throws Exception {
        //   deleteDirectory(new File("Output/case1"));

        JSONParser parser = new JSONParser();

        ArrayList<EndomondoWritable> data = new ArrayList<>();
        JSONObject obj;
        String line = null;
        try {
            Reader reader = new FileReader("C:\\Users\\ASUS\\IdeaProjects\\Proyek\\input\\endomondoHR_proper.json");
            BufferedReader bufferedReader = new BufferedReader(reader);
            while((line = bufferedReader.readLine()) != null) {
                obj = (JSONObject) new JSONParser().parse(line);
                JSONArray longitude = (JSONArray) obj.get("longitude");
                JSONArray altitude = (JSONArray)obj.get("altitude");
                JSONArray latitude = (JSONArray)obj.get("latitude");
                JSONArray heart_rate = (JSONArray)obj.get("heart_rate");
                JSONArray timestamp = (JSONArray)obj.get("timestamp");
                String sport = (String)obj.get("sport");
                String gender = (String)obj.get("gender");
                Long id = (Long)obj.get("id");
                Long userid = (Long)obj.get("userId");
                Double heart=0.0;
                Double longt = 0.0, lat=0.0, alt=0.0;
                Double distance=0.0;
                for(int i=0;i<longitude.size();i++){
                    if(i>0){
                        distance = distance + distanceTo((double)latitude.get(i),(double)longitude.get(i),(double)latitude.get(i-1),(double)longitude.get(i-1));
                    }
                    heart = heart+(long)heart_rate.get(i);
                    lat = lat + (double)latitude.get(i);
                    alt = alt + (double)altitude.get(i);
                    longt = longt + (double)longitude.get(i);
                }
                longt=longt/longitude.size();
                lat=lat/latitude.size();
                alt = alt/altitude.size();
                heart = heart/heart_rate.size();
                long duration = (long)timestamp.get(timestamp.size()-1) - (long)timestamp.get(0);
                long time = (long)timestamp.get(timestamp.size()-1);
                EndomondoWritable end = new EndomondoWritable();
                end.setAltitude(alt);
                end.setGender(gender);
                end.setId(id);
                end.setLatitude(lat);
                end.setLongitude(longt);
                end.setSport(sport);
                end.setUserId(userid);
                end.setDistance(distance);
                end.setHeart_rate(heart);
                end.setTimestamp(time);
                end.setDuration(duration);
                data.add(end);
            }
            bufferedReader.close();

            FileWriter csvWriter = new FileWriter("C:\\Users\\ASUS\\IdeaProjects\\Proyek\\input\\endomondo2.csv");
            csvWriter.append("id");
            csvWriter.append(",");
            csvWriter.append("userId");
            csvWriter.append(",");
            csvWriter.append("Gender");
            csvWriter.append(",");
            csvWriter.append("Sport");
            csvWriter.append(",");
            csvWriter.append("average_heartrate");
            csvWriter.append(",");
            csvWriter.append("timestamp");
            csvWriter.append(",");
            csvWriter.append("distance");
            csvWriter.append(",");
            csvWriter.append("long");
            csvWriter.append(",");
            csvWriter.append("lat");
            csvWriter.append(",");
            csvWriter.append("altitude");
            csvWriter.append(",");
            csvWriter.append("duration");
            csvWriter.append("\n");
            System.out.println(data.size());

            for (int i=0; i<data.size(); i++){
                System.out.println("Data ke - "+i+" - "+data.get(i).getId());
                csvWriter.append(String.valueOf(data.get(i).getId()));
                csvWriter.append(",");
                csvWriter.append(String.valueOf(data.get(i).getUserId()));
                csvWriter.append(",");
                csvWriter.append(data.get(i).getGender());
                csvWriter.append(",");
                csvWriter.append(data.get(i).getSport());
                csvWriter.append(",");
                csvWriter.append(String.valueOf(data.get(i).getHeart_rate()));
                csvWriter.append(",");
                csvWriter.append(String.valueOf(data.get(i).getTimestamp()));
                csvWriter.append(",");
                csvWriter.append(String.valueOf(data.get(i).getDistance()));
                csvWriter.append(",");
                csvWriter.append(String.valueOf(data.get(i).getLongitude()));
                csvWriter.append(",");
                csvWriter.append(String.valueOf(data.get(i).getLatitude()));
                csvWriter.append(",");
                csvWriter.append(String.valueOf(data.get(i).getAltitude()));
                csvWriter.append(",");
                csvWriter.append(String.valueOf(data.get(i).getDuration()));
                csvWriter.append("\n");
            }

            csvWriter.flush();
            csvWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

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
