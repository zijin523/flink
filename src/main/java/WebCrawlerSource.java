import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.util.HashSet;
import java.util.Set;

public class WebCrawlerSource implements SourceFunction<Tuple2<String, String>> {
    private volatile boolean isRunning = true;
    private Set<String> visitedUrls = new HashSet<>();  // 已访问的 URL 集合
    private Set<String> urlsToVisit = new HashSet<>();  // 待访问的 URL 集合

    private String startUrl = "https://nightlies.apache.org/flink/flink-docs-master/"; // 起始 URL

    public WebCrawlerSource() {
        urlsToVisit.add(startUrl);
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        while (isRunning && !urlsToVisit.isEmpty()) {

            String currentUrl = urlsToVisit.iterator().next(); // 取出一个 URL
            urlsToVisit.remove(currentUrl);  // 从待爬取列表中移除

            if (visitedUrls.contains(currentUrl)) {
                continue;  // 如果 URL 已经访问过，则跳过
            }

            try {
                // Jsoup 抓取页面内容
                Document doc = Jsoup.connect(currentUrl).get();
                String title = doc.title();
                String content = doc.body().text(); // 获取页面正文内容

                // 输出抓取到的信息
                ctx.collect(Tuple2.of(currentUrl, "Title: " + title + ", Content: " + content));

                // 将当前 URL 标记为已访问
                visitedUrls.add(currentUrl);

                // 提取页面中的所有链接（相对链接和绝对链接）
                for (Element link : doc.select("a[href]")) {
                    String linkUrl = link.absUrl("href");  // 获取绝对 URL
                    // 如果链接是有效的且没有被访问过，则添加到待爬取列表
                    if (!visitedUrls.contains(linkUrl) && !urlsToVisit.contains(linkUrl)) {
                        urlsToVisit.add(linkUrl);
                    }
                }

                Thread.sleep(3000); // 每抓取一次页面后等待 3 秒

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

