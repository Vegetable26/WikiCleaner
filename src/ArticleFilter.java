import info.bliki.wiki.dump.IArticleFilter;
import info.bliki.wiki.dump.Siteinfo;
import info.bliki.wiki.dump.WikiArticle;
import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Most of the code was taken from:
 * http://trulymadlywordly.blogspot.com/2011/03/creating-text-corpus-from-wikipedia.html
 */

public class ArticleFilter implements IArticleFilter {

    private final static int LIMIT = Integer.MAX_VALUE;   // Set to a lower value to limit the number of pages that are read
    private final static int MAX_QUEUED_TASKS = 100;      // Only allow 100 tasks to be queued at a time, to prevent OOM errors
    private final static int BUFFER_SIZE = 1000000;       // 1 MB
    private final static int NUM_WRITERS = 1;

    // WikiModels are not threadsafe, so map each thread to its own
    private final Map<Long, WikiModel> wikiModels = new HashMap<>();
    private final ConcurrentLinkedQueue<StringBuffer> stringBuffers = new ConcurrentLinkedQueue<>();
    private final AtomicInteger bufferCount = new AtomicInteger();

    private BufferedWriter outputWriter;
    private ThreadPoolExecutor formatWorkers;
    private ThreadPoolExecutor outputWorkers;

    private final static Pattern regex = Pattern.compile("[A-Z][\\p{L}\\w\\p{Blank},\\\"\\';\\[\\]\\(\\)-]+[\\.!]",
            Pattern.CANON_EQ);

    int pageCount = 0;

    public ArticleFilter(Writer writer, int numWorkers) throws FileNotFoundException {
        outputWriter = new BufferedWriter(writer);
        formatWorkers = new ThreadPoolExecutor(numWorkers, numWorkers, Long.MAX_VALUE, TimeUnit.NANOSECONDS,
                new LimitedQueue<>(MAX_QUEUED_TASKS));
        outputWorkers = new ThreadPoolExecutor(NUM_WRITERS, NUM_WRITERS, Long.MAX_VALUE, TimeUnit.NANOSECONDS,
                new LimitedQueue<>(MAX_QUEUED_TASKS));
    }

    public void process(WikiArticle page, Siteinfo siteinfo) throws IOException {
        if (page != null && page.getText() != null && !page.getText().startsWith("#REDIRECT ")) {
            formatWorkers.execute(new ParsePageTask(page));
            if (pageCount++ > LIMIT) {
                try {
                    terminatePipeline();
                } catch (InterruptedException ie) {
                    throw new IOException("Received interrupt exception while shutting down pipeline at " + pageCount + " pages");
                }
                throw new IOException("Terminating program at " + pageCount + " pages");
            }
        }
    }

    /**
     * Spin until the worker queues have completed all their tasks
     */
    public void terminatePipeline() throws InterruptedException {
        formatWorkers.shutdown();
        while(!formatWorkers.isTerminated()) {
            formatWorkers.awaitTermination(1000, TimeUnit.MILLISECONDS);
        }
        outputWorkers.shutdown();
        while (!outputWorkers.isTerminated()) {
            outputWorkers.awaitTermination(1000, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Task to parse and format a wikipedia page using REGEX
     */
    private class ParsePageTask implements Runnable {

        private WikiArticle page;

        private ParsePageTask(WikiArticle page) {
            this.page = page;
        }

        @Override
        public void run() {

            long threadId = Thread.currentThread().getId();
            if (!wikiModels.containsKey(threadId)) {
                wikiModels.put(threadId, new WikiModel("${image}", "${title}"));
            }
            WikiModel wikiModel = wikiModels.get(threadId);

            StringBuffer stringBuffer;
            synchronized (stringBuffers) {
                // Only create string buffers if necessary
                if (stringBuffers.isEmpty() ) {
                    stringBuffers.add(new StringBuffer(BUFFER_SIZE));
                    System.out.println("ADDING NEW BUFFER " + bufferCount.incrementAndGet());
                }
                stringBuffer = stringBuffers.poll();
                stringBuffer.setLength(0);
            }
            try {
                String timestamp = parseTime(page.getTimeStamp());

                // Zap headings ==some text== or ===some text===

                // <ref>{{Cite web|url=http://tmh.floonet.net/articles/falseprinciple.html |title="The False Principle of our Education" by Max Stirner |publisher=Tmh.floonet.net |date= |accessdate=2010-09-20}}</ref>
                // <ref>Christopher Gray, ''Leaving the Twentieth Century'', p. 88.</ref>
                // <ref>Sochen, June. 1972. ''The New Woman: Feminism in Greenwich Village 1910Ð1920.'' New York: Quadrangle.</ref>

                // String refexp = "[A-Za-z0-9+\\s\\{\\}:_=''|\\.\\w#\"\\(\\)\\[\\]/,?&%Ð-]+";

                String wikiText = page.getText().
                        replaceAll("[=]+[A-Za-z+\\s-]+[=]+", " ").
                        replaceAll("\\{\\{[A-Za-z0-9+\\s-]+\\}\\}", " ").
                        replaceAll("(?m)<ref>.+</ref>", " ").
                        replaceAll("(?m)<ref name=\"[A-Za-z0-9\\s-]+\">.+</ref>", " ").
                        replaceAll("<ref>", " <ref>");

                // Remove text inside {{ }}
                String plainStr = wikiModel.render(new PlainTextConverter(), wikiText).
                        replaceAll("\\{\\{[A-Za-z+\\s-]+\\}\\}", " ");

                Matcher regexMatcher = regex.matcher(plainStr);
                while (regexMatcher.find()) {
                    String output = regexMatcher.group();
                    stringBuffer.append(output.toLowerCase().replaceAll("[\\p{Punct}\\s]+", " "));
                }
                stringBuffer.append("\n");
                outputWorkers.execute(new OutputTask(timestamp, stringBuffer));
            } catch (Exception e) {
                // e.printStackTrace();
                System.out.println("FAILED ON " + page.getTitle());
                e.printStackTrace();
            }
        }

        private String parseTime(String rawTime) {
            // TODO: Process the time
            return rawTime;
        }
    }

    /**
     * Task to output the newly formatted data to the output file
     */
    private class OutputTask implements Runnable {

        private String time;
        private StringBuffer wordStream;

        public OutputTask(String time, StringBuffer wordStream) {
            this.time = time;
            this.wordStream = wordStream;
        }

        @Override
        public void run() {
            try {
                synchronized (outputWriter) {
                    outputWriter.write(time + "\n");
                    outputWriter.append(wordStream);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            synchronized (wordStream) {
                stringBuffers.add(wordStream);
            }
        }
    }

    // http://stackoverflow.com/questions/4521983/java-executorservice-that-blocks-on-submission-after-a-certain-queue-size
    public class LimitedQueue<E> extends LinkedBlockingQueue<E> {
        public LimitedQueue(int maxSize)
        {
            super(maxSize);
        }

        @Override
        public boolean offer(E e)
        {
            // turn offer() and add() into a blocking calls (unless interrupted)
            try {
                put(e);
                return true;
            } catch(InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
    }
}
