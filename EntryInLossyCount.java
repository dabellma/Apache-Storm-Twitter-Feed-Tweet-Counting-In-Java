public class EntryInLossyCount {

    private String element;
    private int frequency;
    private int error;

    public EntryInLossyCount(String element, int frequency, int error) {
        this.element = element;
        this.frequency = frequency;
        this.error = error;
    }

    public String getElement() {
        return this.element;
    }

    public int getFrequency() {
        return this.frequency;
    }

    public int getError() {
        return this.error;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }
}
