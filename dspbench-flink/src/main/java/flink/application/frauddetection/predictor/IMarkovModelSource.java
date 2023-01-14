package flink.application.frauddetection.predictor;

/**
 *
 */
public interface IMarkovModelSource {
    String getModel(String key);
}
