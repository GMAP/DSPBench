package com.streamer.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JavaUtils.class);
    
    public static boolean isJar() {
        String className = JavaUtils.class.getName().replace('.', '/');
        String classJar = JavaUtils.class.getResource("/" + className + ".class").toString();
        
        return classJar.startsWith("jar:");
    }
    
    public static String getHostname() {
        try {
            Process p = Runtime.getRuntime().exec("hostname");
            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            return input.readLine();
        } catch (IOException ex) {
            LOG.error("Unable to get hostname", ex);
            return "";
        }
    }
}