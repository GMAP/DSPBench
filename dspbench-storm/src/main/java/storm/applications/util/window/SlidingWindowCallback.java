package storm.applications.util.window;

import java.util.List;

/**
 * Author: Thilina
 * Date: 11/22/14
 */
public interface SlidingWindowCallback {
    public void remove(List<SlidingWindowEntry> entries);
}