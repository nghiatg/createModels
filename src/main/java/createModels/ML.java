package createModels;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class ML {
	static SparkConf conf = new SparkConf().setAppName("create-models").setMaster("local[1]");
	static JavaSparkContext jsc = new JavaSparkContext(conf);
	static SQLContext sqlc = new SQLContext(jsc);

	public static void tfidf(String input, String vocabPath, String idfModelPath, String cvModelPath) throws Exception {
		DataFrame rawDF = Utils.createDataSet(input);
		CountVectorizerModel cvModel = new CountVectorizer().setInputCol("raw").setOutputCol("tf").setMinDF(1).fit(rawDF);
		cvModel.save(cvModelPath);
		DataFrame afterCV = cvModel.transform(rawDF);
		IDFModel idfModel = new IDF().setInputCol("tf").setOutputCol("features").fit(afterCV);
		idfModel.save(idfModelPath);
		Utils.saveVocab(cvModel.vocabulary(), vocabPath);
	}
}
