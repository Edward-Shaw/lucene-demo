/**
 * Created by He on 2016/3/7.
 */

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.Version;

import com.mongodb.*;

/**
 * This class demonstrate the process of creating index with Lucene
 * for text files
 */
public class FileIndexer {

	public static void main(String[] args) throws IOException {
		//createIndex();
		//createdbIndex();

		Scanner in = new Scanner(System.in);
		System.out.println("输入检测字段");
		String type = in.nextLine();
		System.out.println("输入检测关键字");
		String word = in.nextLine();
		if (type != "" && word != "")
			queryLucene(word, type);

	}

	public static void queryLucene(String keyword, String type) {
		try {
			File path = new File("C:\\Users\\He\\IntelliJProjects\\TestLucene\\index");
			Directory mdDirectory = FSDirectory.open(path);
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);

			IndexReader reader = IndexReader.open(mdDirectory);
			IndexSearcher searcher = new IndexSearcher(reader);

			// (在多个Filed中搜索)
			//String[] fields = {"filename", "content"};
			//QueryParser queryParser = new MultiFieldQueryParser(Version.LUCENE_30, fields, analyzer);
			//Query query = queryParser.parse(keyword);

			//规定搜索范围
			//QueryParser queryParser = new QueryParser(Version.LUCENE_30, "filename", analyzer);
			QueryParser queryParser = new QueryParser(Version.LUCENE_30, type, analyzer);
			Query query = queryParser.parse(keyword);

			long start = System.currentTimeMillis();

			ScoreDoc[] docs = searcher.search(query, 5).scoreDocs;

			for (ScoreDoc sd : docs) {
				Document doc = searcher.doc(sd.doc);
				System.out.println(doc.get("filename") + "[" + doc.get("filepath") + "]");
				//System.out.println(doc.get("name") + " " + doc.get("content"));
			}

			reader.close();
			searcher.close();

			long end = System.currentTimeMillis();
			System.out.println("queryLucene耗时：" + (end - start) + "ms");

		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 创建索引
	 */
	private static void createIndex() {

		try {

			File path = new File("C:\\Users\\He\\IntelliJProjects\\TestLucene\\index");
			Directory mdDirectory = FSDirectory.open(path);
			//lucene2.9版本后，索引格式不再兼容所有版本，需要指定匹配的版本号
			Analyzer mAnalyzer = new StandardAnalyzer(Version.LUCENE_30);
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_30, mAnalyzer);
			IndexWriter writer = new IndexWriter(mdDirectory, config);

			long start = System.currentTimeMillis();

			File[] files = new File("C:\\Users\\He\\IntelliJProjects\\TestLucene\\file").listFiles();
			for (File file : files) {

				//Document存储索引的信息
				Document doc = new Document();

//				Field.Store.YES:存储字段值（未分词前的字段值）
//				Field.Store.NO:不存储,存储与索引没有关系
//				Field.Store.COMPRESS:压缩存储,用于长文本或二进制，但性能受损
//
//				Field.Index.ANALYZED:分词建索引
//				Field.Index.ANALYZED_NO_NORMS:分词建索引，但是Field的值不像通常那样被保存，而是只取一个byte，这样节约存储空间
//				Field.Index.NOT_ANALYZED:不分词且索引 适用于精确搜索
//				Field.Index.NOT_ANALYZED_NO_NORMS:不分词建索引，Field的值去一个byte保存
				Field name = new Field("filename", file.getName(), Store.YES, Index.ANALYZED);
				Field content = new Field("content", new FileReader(file));
				Field abspath = new Field("filepath", file.getAbsolutePath(), Store.YES, Index.ANALYZED);

				doc.add(name);
				doc.add(content);
				doc.add(abspath);

				writer.addDocument(doc);
			}

			writer.close();

			long end = System.currentTimeMillis();
			System.out.println("createIndex耗时：" + (end - start) + "ms");

		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (LockObtainFailedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void createdbIndex() {
		try {
			MongoClient mongoClient = new MongoClient("localhost", 27017);
			DB db = mongoClient.getDB("Test");
			DBCollection msg = db.getCollection("check");
			DBCursor cursor = msg.find();
			boolean exist = cursor.hasNext();

			File path = new File("C:\\Users\\He\\IntelliJProjects\\TestLucene\\dbindex");
			Directory mdDirectory = FSDirectory.open(path);
			//lucene2.9版本后，索引格式不再兼容所有版本，需要指定匹配的版本号
			Analyzer mAnalyzer = new StandardAnalyzer(Version.LUCENE_30);
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_30, mAnalyzer);
			IndexWriter writer = new IndexWriter(mdDirectory, config);

			while (exist) {
				Document doc = new Document();
				DBObject tmpcursor = cursor.next();
				Field name = new Field("name", tmpcursor.get("name").toString(), Field.Store.YES, Field.Index.ANALYZED);
				Field content = new Field("content", tmpcursor.get("content").toString(), Field.Store.YES, Field.Index.ANALYZED);
				doc.add(name);
				doc.add(content);
				//System.out.println(cursor.next().get("name"));
				writer.addDocument(doc);
				exist = cursor.hasNext();
			}

			writer.close();

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}

}