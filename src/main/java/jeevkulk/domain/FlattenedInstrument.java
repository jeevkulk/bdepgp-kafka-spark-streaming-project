package jeevkulk.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonRootName;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@JsonRootName(value = "flattenedInstrument")
public class FlattenedInstrument implements Serializable {
    private static final long serialVersionUID = 1L;

    private String symbol;
    private Timestamp eventTimestamp;
    private double open;
    private double close;
    private double high;
    private double low;
    private double volume;
    @JsonIgnore
    private int dataPointsCount;

    public FlattenedInstrument() { }

    public FlattenedInstrument(Instrument instrument) {
        this.setSymbol(instrument.getSymbol());
        this.setOpen(instrument.getPriceData().getOpen());
        this.setClose(instrument.getPriceData().getClose());
        this.setLow(instrument.getPriceData().getLow());
        this.setHigh(instrument.getPriceData().getHigh());
        this.setVolume(Math.abs(instrument.getPriceData().getVolume()));
        try {
            DateFormat formatter = new SimpleDateFormat("yyyy-M-dd hh:mm:ss");
            Date date = formatter.parse(instrument.getTimestamp());
            this.setEventTimestamp(new Timestamp(date.getTime()));
        } catch (ParseException ex) {
            // Throw exception
        }
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public Timestamp getEventTimestamp() {
        return eventTimestamp;
    }

    public void setEventTimestamp(Timestamp eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    public double getOpen() {
        return open;
    }

    public void setOpen(double open) {
        this.open = open;
    }

    public double getClose() {
        return close;
    }

    public void setClose(double close) {
        this.close = close;
    }

    public double getHigh() {
        return high;
    }

    public void setHigh(double high) {
        this.high = high;
    }

    public double getLow() {
        return low;
    }

    public void setLow(double low) {
        this.low = low;
    }

    public double getVolume() {
        return volume;
    }

    public void setVolume(double volume) {
        this.volume = volume;
    }

    public int getDataPointsCount() {
        return dataPointsCount;
    }

    public void setDataPointsCount(int dataPointsCount) {
        this.dataPointsCount = dataPointsCount;
    }

    @Override
    public String toString() {
        return "FlattenedInstrument{" +
                "symbol='" + symbol + '\'' +
                ", eventTimestamp=" + eventTimestamp +
                ", open=" + open +
                ", close=" + close +
                ", high=" + high +
                ", low=" + low +
                ", volume=" + volume +
                ", dataPointsCount=" + dataPointsCount +
                '}';
    }
}
