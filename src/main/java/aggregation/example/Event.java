package aggregation.example;


public class Event {

    private int sensorID;
    private long value;

    public int getSensorID() {
        return sensorID;
    }

    public void setSensorID(int sensorID) {
        this.sensorID = sensorID;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public String toString() {
        return sensorID + "," + value;
    }
}
