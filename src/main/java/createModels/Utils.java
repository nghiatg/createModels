package createModels;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Utils {
	public static DataFrame createDataSet(String input) throws Exception {
		ArrayList<Row> rows = new ArrayList<Row>();
		BufferedReader br = new BufferedReader(new FileReader(input));
		String line = br.readLine();
		int id = 0;
		while(line != null) {
			rows.add(changeStringToRow(line, id));
			id++;
			line = br.readLine();
		}
		br.close();
		StructType schema = new StructType(new StructField[] {
				new StructField("id", DataTypes.LongType, false, Metadata.empty()),
				new StructField("raw", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty()) });
		DataFrame dataset = ML.sqlc.createDataFrame(rows, schema);
		return dataset;

	}
	
	public static Row changeStringToRow(String doc, int id) throws IOException {
		ArrayList<String> words = new ArrayList<String>();
		for(String w : doc.split(" ")) {
			if(w.length() <= 0) {
				continue;
			}
			words.add(w);
		}
		Row r = RowFactory.create(new Long(id), words);
		return r;
	}
	
	public static void saveVocab(String vocab[], String vocabPath) throws Exception { 
		PrintWriter pr = new PrintWriter(vocabPath);
		for(int i = 0 ; i < vocab.length ; ++i) {
			pr.println(vocab[i]);
		}
		pr.close();
	}
	
	public static void deleteDir(File file) {
	    File[] contents = file.listFiles();
	    if (contents != null) {
	        for (File f : contents) {
	            deleteDir(f);
	        }
	    }
	    file.delete();
	}
}
