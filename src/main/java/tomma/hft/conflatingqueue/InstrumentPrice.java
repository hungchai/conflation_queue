package tomma.hft.conflatingqueue;

public class InstrumentPrice implements KeyValue<String, Long>{

    private String instrument;
    private long price;

    public InstrumentPrice(String instrument, long price) {
        this.instrument = instrument;
        this.price = price;
    }

    @Override
    public String getKey() {
        return instrument;
    }

    @Override
    public Long getValue() {
        return price;
    }

    public void setPrice(long price) {
        this.price = price;
    }
}
