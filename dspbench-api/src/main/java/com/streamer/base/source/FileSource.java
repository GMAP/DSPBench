package com.streamer.base.source;


import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.streamer.core.Values;
import com.streamer.base.constants.BaseConstants.BaseConfig;
import com.streamer.base.source.parser.Parser;
import com.streamer.metrics.MetricsFactory;
import com.streamer.metrics.Progress;
import com.streamer.utils.ClassLoaderUtils;
import com.streamer.utils.FileUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSource extends BaseSource {
    private static final Logger LOG = LoggerFactory.getLogger(FileSource.class);
        
    private Parser parser;
    private File[] files;
    private File currFile;
    private BufferedReader reader;
    private int curFileIndex   = 0;
    private boolean finished   = false;
    
    private long totalBytes = 0;
    private Counter readBytes;
    private Counter msgCount;
    private Progress progress;
    
    public void initialize() {
        String parserClass = config.getString(getConfigKey(BaseConfig.SOURCE_PARSER));
        parser = (Parser) ClassLoaderUtils.newInstance(parserClass, "parser", LOG);
        parser.initialize(config);
        
        String prefix = String.format("%s-%d", config.getString(getConfigKey(BaseConfig.SOURCE_PRODUCER_PREFIX), "producer"), id);
        
        MetricRegistry metrics = MetricsFactory.createRegistry(config);
        if (metrics != null) {
            readBytes = metrics.counter(prefix + ".read");
            msgCount  = metrics.counter(prefix + ".messages");
            progress = metrics.register(prefix + ".progress", new Progress());
        }
        
        buildIndex();
        openNextFile();
    }
    
    protected void buildIndex() {
        String path = config.getString(getConfigKey(BaseConfig.SOURCE_PATH));
        if (StringUtils.isBlank(path)) {
            LOG.error("The source path has not been set");
            throw new RuntimeException("The source path has to beeen set");
        }

        LOG.info("Source path: {}", path);
        
        File dir = new File(path);
        if (!dir.exists()) {
            LOG.error("The source path {} does not exists", path);
            throw new RuntimeException("The source path '" + path + "' does not exists");
        }
        
        if (dir.isDirectory()) {
            files = dir.listFiles();
        } else {
            files = new File[1];
            files[0] = dir;
        }
        
        Arrays.sort(files, new Comparator<File>() {
            public int compare(File f1, File f2) {
                int res = f1.lastModified() < f2.lastModified() ? -1 : ( f1.lastModified() > f2.lastModified() ? 1 : 0);
                return res;
            }
        });
        
        for (File f : files) {
            totalBytes += f.length();
        }
        
        progress.setTotal(totalBytes);
        
        LOG.info("Producer has to read {} files, totalizing {}", files.length, FileUtils.humanReadableByteCount(totalBytes));
    }

    @Override
    public void nextTuple() {
        try {
            String value = readLine();

            if (value == null)
                return;

            List<Values> tuples = parser.parse(files[curFileIndex].getName(), value);

            if (tuples != null) {
                for (Values values : tuples) {
                    emit(values.getStreamId(), values);
                    msgCount.inc();
                }
            }
        } catch (IOException ex) {
            LOG.error("Error while reading file " + files[curFileIndex].getName(), ex);
        }
    }
    
    @Override
    public boolean hasNext() {
        return !finished;
    }

    private String readLine() throws IOException {
        if (finished) return null;
        
        String record = reader.readLine();
        
        if (record == null) {
            if (++curFileIndex < files.length) {
                LOG.info("File {} finished", files[curFileIndex]);
                openNextFile();
                record = reader.readLine();			 
            } else {
                LOG.info("No more files to read");
                finished = true;
            }
        }
        return record;
    }

    private void openNextFile() {
        try {
            if (currFile != null) {
                readBytes.inc(currFile.length());
                progress.setProgress(readBytes.getCount());
            }
            
            currFile = files[curFileIndex];
            reader = new BufferedReader(new FileReader(currFile));
            LOG.info("Opened file {}, size {}", currFile.getName(), 
                    FileUtils.humanReadableByteCount(currFile.length()));
            
        } catch (FileNotFoundException e) {
            LOG.error(String.format("File %s not found", files[curFileIndex]), e);
            throw new IllegalStateException("file not found");
        }
    }
}	
