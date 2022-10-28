package flink;

import flink.application.AbstractApplication;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

public class AppDriver {

    private static final Logger LOG = LoggerFactory.getLogger(AppDriver.class);
    private final Map<String, AppDescriptor> applications;

    public AppDriver() {
        applications = new HashMap<>();
    }

    public void addApp(String name, Class<? extends AbstractApplication> cls) {
        applications.put(name, new AppDescriptor(cls));
    }

    public AppDescriptor getApp(String name) {
        return applications.get(name);
    }

    public static class AppDescriptor {
        private final Class<? extends AbstractApplication> cls;

        public AppDescriptor(Class<? extends AbstractApplication> cls) {
            this.cls = cls;
        }

        public StreamExecutionEnvironment getContext(String applicationName, Configuration config) {
            try {
                Constructor c = cls.getConstructor(String.class, Configuration.class);
                LOG.warn("Loaded application {}", cls.getCanonicalName());

                AbstractApplication application = (AbstractApplication) c.newInstance(applicationName, config);

                application.initialize();
                return application.buildApplication();
            } catch (ReflectiveOperationException ex) {
                LOG.error("Unable to load application class {}", ex);
                return null;
            }
        }
    }
}
