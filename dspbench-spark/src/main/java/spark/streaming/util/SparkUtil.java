package spark.streaming.util;



import scala.Tuple2;
import scala.Product2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;


/**
 * @author billg
 */
public class SparkUtil {

    public static <T> ClassTag<T> getManifest(Class<T> clazz) {
        return ClassTag$.MODULE$.apply(clazz);
    }

    public static <K,V> ClassTag<Tuple2<K, V>> getTuple2Manifest() {
        return (ClassTag<Tuple2<K, V>>)(Object)getManifest(Tuple2.class);
    }
    
    public static <K,V> ClassTag<Product2<K, V>> getProduct2Manifest() {
        return (ClassTag<Product2<K, V>>)(Object)getManifest(Product2.class);
    }


}