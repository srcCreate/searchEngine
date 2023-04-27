package searchengine.dto.indexing;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.data.domain.Page;
import searchengine.config.Site;
import searchengine.config.SitesList;
import searchengine.model.*;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.http.HttpResponse;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;


/*
    Данный класс пока не используется, пока не пойму нужен ли будет вообще.
 */

@Data
public class PageIndexer extends RecursiveTask<String> {
    private final List<Site> siteList = new ArrayList<>();
    private final SiteEntity indexingSite;
    private String url;

//    public PageIndexer(String url) {
//        this.url = url;
//    }

    @Override
    protected String compute() {
//        Connection.Response response;
//        Document document;
//        try {
//            response = Jsoup.connect(url)
//                    .userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) " +
//                            "Gecko/20070725 Firefox/2.0.0.6")
//                    .timeout(10000)
//                    .execute();
//            document = response.parse();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//
//        Elements elements = document.select("a");
//        Set<PageEntity> pages = new HashSet<>();
//
//        Set<String> links = new HashSet<>();
//
//        for (Element element : elements) {
//            String attr = element.attr("abs:href");
//            if (!attr.isEmpty() && attr.startsWith(url) && !attr.contains("#")) {
//                try {
//                    response = Jsoup.connect(attr)
//                            .userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) " +
//                                    "Gecko/20070725 Firefox/2.0.0.6")
//                            .timeout(10000)
//                            .execute();
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//                String attr1 = attr.replace(url, "");
//                int lastChar = attr1.length() - 1;
//                if (!attr1.trim().isEmpty() && attr1.charAt(lastChar) == '/') {
//                    attr1 = attr1.substring(0, lastChar);
//                }
//
//                if (links.add(attr1)) {
//                    PageEntity indexingPage = new PageEntity();
//                    indexingPage.setSiteId(indexingSite);
//                    indexingPage.setPath(attr1);
//                    indexingPage.setContent(document.html());
//                    indexingPage.setCode(response.statusCode());
//
//                    pages.add(indexingPage);
//                }
//            }
//    }
//}
//    private Set<PageEntity> createPage(Site site) {
//        HashSet<PageEntity> pages = new HashSet<>();
//        String url = site.getUrl();
//
//        return pages;
//    }
        return null;
    }
}
