/**
 * Acknowledgement: Code template (Main, JobMapReduce) for running mapReduce jobs
 * was developed within lab5 of BDM course on BDMA semester 2, taken place in UPC, Barcelona.
 */
import java.io.IOException;

public abstract class JobMapReduce {

    protected String input;
    protected String output;

    public void setInput(String input) {
        this.input = input;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
        return false;
    }

}