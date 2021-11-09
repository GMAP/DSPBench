package spark.streaming.receiver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class FileReceiver extends Receiver<Tuple2<String, Tuple>> implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(FileReceiver.class);
    
    private final String path;
    
    private File[] files;
    private File currFile;
    private BufferedReader reader;
    private int curFileIndex   = 0;
    private boolean finished   = false;
    private long lines = 0;
    
    public FileReceiver(String path) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        
        this.path = path;
    }
    
    protected void buildIndex() {
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
            @Override
            public int compare(File f1, File f2) {
                int res = f1.lastModified() < f2.lastModified() ? -1 : ( f1.lastModified() > f2.lastModified() ? 1 : 0);
                return res;
            }
        });
        
        LOG.info("{} files to read", files.length);
        System.out.println(files.length + " files to read");
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
            currFile = files[curFileIndex];
            reader = new BufferedReader(new FileReader(currFile));          
            System.out.println("Opened file "+currFile.getName());
        } catch (FileNotFoundException e) {
            LOG.error(String.format("File %s not found", files[curFileIndex]), e);
            throw new IllegalStateException("file not found");
        }
    }

    @Override
    public void onStart() {
        buildIndex();
        openNextFile();
        
        new Thread(this).start();
        
        //ExecutorService executor = Executors.newSingleThreadExecutor();
        //executor.submit(this);
    }

    @Override
    public void onStop() {
        
    }

    @Override
    public void run() {
        while (!finished) {
            try {
                String value = readLine();

                if (!StringUtils.isBlank(value)) {
                    store(new Tuple2<>(value, new Tuple()));
                    lines++;
                }
            } catch (IOException ex) {
                LOG.error("Error reading line from "+currFile.getName(), ex);
            }
        }
        
        System.out.println("Total lines: " + lines);
    }
    
}
