package com.streamer.topology;

import com.streamer.core.Component;
import com.streamer.core.hook.Hook;
import java.io.Serializable;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 * @param <T>
 */
public interface IComponentAdapter<T extends Component> extends Serializable {
    public void setComponent(T operator);
    public T getComponent();
    public void addComponentHook(Hook hook);
}
