package flink.application.frauddetection.predictor;

/**
 *
 */
public interface IMarkovModelSource {
    public String getModel(String key);
}
