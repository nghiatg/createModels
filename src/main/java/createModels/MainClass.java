package createModels;

import java.io.File;

public class MainClass {
	static String vocabPath = "data//vocabulary";
	static String input = "data//input";
	static String cvModelPath = "models//cvModel";
	static String idfModelPath = "models//idfModel";

	public static void main(String[] args) throws Exception {
		try {
			Utils.deleteDir(new File(cvModelPath));
		}catch(Exception e) {}
		
		
		try {
			Utils.deleteDir(new File(idfModelPath));
		}catch(Exception e) {}
		
		
		ML.tfidf(input, vocabPath, idfModelPath, cvModelPath);
	}

}
