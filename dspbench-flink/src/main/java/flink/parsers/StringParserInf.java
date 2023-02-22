package flink.parsers;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringParserInf extends Parser implements FlatMapFunction<String, Tuple1<String>> {

    private static final Logger LOG = LoggerFactory.getLogger(StringParser.class);

    Configuration config;

    public StringParserInf(Configuration config){
        super.initialize(config);
        this.config = config;
    }

    @Override
    public void flatMap(String value, Collector<Tuple1<String>> out) {
        super.initialize(config);

        try {
			Scanner scanner = new Scanner(new File(value));

			while (scanner.hasNextLine()) {
                super.incBoth();
                String line = scanner.nextLine();
                if(!scanner.hasNextLine()){
                    scanner = new Scanner(new File(value));
                }
                if (StringUtils.isBlank(line))
                    out.collect(new Tuple1<String>(null));

                out.collect(new Tuple1<String>(line));
			}

			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
    }

    @Override
    public Tuple1<?> parse(String input) {
        return null;
    }
}
