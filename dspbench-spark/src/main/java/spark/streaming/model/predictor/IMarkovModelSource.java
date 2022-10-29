package spark.streaming.model.predictor;

/**
 *
 * @author maycon
 */
public interface IMarkovModelSource {
    public String getModel(String key);
}
