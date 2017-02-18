import java.io.*;
import java.util.concurrent.ExecutorService;

import info.bliki.wiki.dump.IArticleFilter;
import info.bliki.wiki.dump.WikiXMLParser;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;

public class WikiMediaCleaner {

    private final static String DATA_DIR = System.getProperty("user.dir") + "/data/";

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: WikiFormatter <input_file> <output_file> <num_threads>");
            return;
        }
        try {
            // Necessary to allow the XMLReader to not throw a security exception
            System.setProperty("jdk.xml.totalEntitySizeLimit", "0");
            File dumpfile = new File(DATA_DIR + args[0]);
            ArticleFilter handler = new ArticleFilter(new PrintWriter(new File(DATA_DIR + args[1])), Integer.parseInt(args[2]));
            WikiXMLParser wxp = new WikiXMLParser(dumpfile, handler);
            wxp.parse();
            handler.terminatePipeline();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Wiki format failed with: " + e.getMessage());
        }
    }
}