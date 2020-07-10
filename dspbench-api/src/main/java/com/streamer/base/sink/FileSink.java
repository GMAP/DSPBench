package com.streamer.base.sink;

import com.streamer.base.constants.BaseConstants.BaseConfig;
import com.streamer.core.Tuple;
import com.streamer.utils.JavaUtils;
import com.streamer.utils.StringUtil;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class FileSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(FileSink.class);
            
    protected BufferedWriter writer = null;
    protected String filename;

    @Override
    public void initialize() {
        filename = config.getString(getConfigKey(BaseConfig.SINK_PATH));
        
        Charset encoding;
        try {
            encoding = Charset.forName(getConfigKey(BaseConfig.SINK_ENCODING));
        } catch (Exception ex) {
            encoding = Charset.forName("UTF-8");
        }
        
        Map<String, Object> map = new HashMap<String, Object>(2);
        map.put("id", getId());
        map.put("name", getName());
        map.put("hostname", JavaUtils.getHostname());
        
        filename = StringUtil.dictFormat(filename, map);

        try {
            writer = new BufferedWriter(new OutputStreamWriter(
                  new FileOutputStream(filename), encoding));
        } catch (FileNotFoundException ex) {
            LOG.error("Unable to find file " + filename, ex);
        }
    }

    @Override
    public void process(Tuple tuple) {
        try {
            writer.write(formatter.format(tuple));
            writer.newLine();
        } catch (IOException ex) {
            LOG.error("Error while writing to file " + filename, ex);
        }
    }

    @Override
    public void onDestroy() {
        super.onDestroy();
        
        try {
            writer.flush();
            writer.close();
        } catch (IOException ex) {
            LOG.error("Error while closing the file " + filename, ex);
        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

}