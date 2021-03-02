public class Endomondo {
    Double[] longitude, altitude, latitude;
    Long[] heart_rate;
    Double distance,location;
    String sport;
    Long id, duration;
    String gender;
    Long userId;


    public Endomondo(){};
    public Endomondo(Double[] longitude, Double[] altitude, Double[] latitude, Long[] heart_rate, Double distance, String sport, Long id, Long duration, String gender, Long userId) {
        this.longitude = longitude;
        this.altitude = altitude;
        this.latitude = latitude;
        this.heart_rate = heart_rate;
        this.distance = distance;
        this.sport = sport;
        this.id = id;
        this.duration = duration;
        this.gender = gender;
        this.userId = userId;
    }
    public void print(){
        System.out.println("ID : "+id);
        System.out.println("User ID : " +userId);
        System.out.println("Sport : "+sport);
        System.out.println("Gender : "+gender);
        System.out.println("Location from User Centroid : "  +location);
        System.out.println("Duration : "+duration);
        System.out.println("Distance : "+distance);
        System.out.println("Heart Rate : "+getAvgHeart());
        System.out.println("Latitude,Longitude : ");
        int co=0;
        for(int i=0;i<longitude.length;i++){
            if(i % 25 == 0 || i==longitude.length-1){
                co+=1;
                System.out.print(" ("+latitude[i]+", "+longitude[i]+"), ");
            }
            if(i==longitude.length){
                System.out.println("");
                System.out.println(co);
            }

        }
    }
    public Double getLocation() {
        return location;
    }

    public void setLocation(Double location) {
        this.location = location;
    }
    public double getAvgHeart(){
        Double avg=0.0;
        for(int i =0; i<heart_rate.length;i++){
            avg=avg+heart_rate[i];
        }
        avg=avg/heart_rate.length;
        return avg;
    }
    public double getLon(){
        Double avg=0.0;
        for(int i =0; i<longitude.length;i++){
            avg=avg+longitude[i];
        }
        avg=avg/longitude.length;
        return avg;
    }
    public double getLat(){
        Double avg=0.0;
        for(int i =0; i<latitude.length;i++){
            avg=avg+latitude[i];
        }
        avg=avg/latitude.length;
        return avg;
    }
    public Double[] getLongitude() {
        return longitude;
    }

    public void setLongitude(Double[] longitude) {
        this.longitude = longitude;
    }

    public Double[] getAltitude() {
        return altitude;
    }

    public void setAltitude(Double[] altitude) {
        this.altitude = altitude;
    }

    public Double[] getLatitude() {
        return latitude;
    }

    public void setLatitude(Double[] latitude) {
        this.latitude = latitude;
    }

    public Long[] getHeart_rate() {
        return heart_rate;
    }

    public void setHeart_rate(Long[] heart_rate) {
        this.heart_rate = heart_rate;
    }

    public Double getDistance() {
        return distance;
    }

    public void setDistance(Double distance) {
        this.distance = distance;
    }

    public String getSport() {
        return sport;
    }

    public void setSport(String sport) {
        this.sport = sport;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }
}