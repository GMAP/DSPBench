package com.streamer.examples.utils;

import java.util.Calendar;
import java.util.Date;

public class DateUtils {
    public static long getMinuteForTime(Date time) {
        Calendar c = Calendar.getInstance();
        c.setTime(time);
        c.set(Calendar.SECOND,0);
        c.set(Calendar.MILLISECOND, 0);
        return c.getTimeInMillis();
    }
}
