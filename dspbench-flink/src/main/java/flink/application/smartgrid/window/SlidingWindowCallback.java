package flink.application.smartgrid.window;

import java.util.List;

import flink.util.Metrics;

/**
 * Author: Thilina
 * Date: 11/22/14
 */
public class SlidingWindowCallback extends Metrics{
    public void remove(List<SlidingWindowEntry> entries){};
}