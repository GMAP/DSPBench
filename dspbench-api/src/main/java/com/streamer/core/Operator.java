package com.streamer.core;

import com.streamer.utils.Configuration;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class Operator extends Component {
    private List<Stream> inputStreams = new ArrayList<Stream>();
    
    @Override
    public void onCreate(int id, Configuration config) {
        super.onCreate(id, config);
    }
    
    @Override
    public void onDestroy() {
        super.onDestroy();
    }

    public void onTime() { }
    public abstract void process(Tuple tuple);

    public void addInputStream(Stream stream) {
        inputStreams.add(stream);
    }

    public List<Stream> getInputStreams() {
        return inputStreams;
    }
}
