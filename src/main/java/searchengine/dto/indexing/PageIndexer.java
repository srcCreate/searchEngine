package searchengine.dto.indexing;

import lombok.Data;
import lombok.RequiredArgsConstructor;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

@Data
public class PageIndexer extends RecursiveTask<String> {
    private final SitesList sites;
//    private String url;
    private static CopyOnWriteArrayList<String> links = new CopyOnWriteArrayList<>();
    private PageRepository pageRepository;
    private SiteRepository siteRepository;

//    public PageIndexer(String url) {
//        this.url = url;
//    }


    public void parse() {
        List<Site> sitesList = sites.getSites();
        for (Site site : sitesList) {
            SiteEntity indexingSite = createSiteEntity(site);
            siteRepository.save(indexingSite);
        }
    }

    @Override
    protected String compute() {
//        List<Site> sitesList = sites.getSites();
//        for (Site site : sitesList) {
//            SiteEntity indexingSite = createSiteEntity(site);
//            siteRepository.save(indexingSite);
//        }


//        StringBuffer sb = new StringBuffer(url + "\n");
//        Set<String> task = new HashSet<>();
//        Document document;
//        Elements elements;
//        try {
//            document = Jsoup.connect(url)
//                    .userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) " +
//                            "Gecko/20070725 Firefox/2.0.0.6").referrer("http://www.google.com").get();
//            elements = document.select("a");
//            for (Element element : elements) {
//                String attr = element.attr("abs:href");
//                if (!attr.isEmpty() && attr.startsWith(url) && !links.contains(attr) && !attr.contains("#")) {
//                    String attr1 = attr.replace(url, "");
//
//                    PageEntity indexingPage = createPage(attr1);
//                    pageRepository.save(indexingPage);
//
////                    if (!attr1.trim().isEmpty()) {
////                        attr1 = attr1.substring(0, attr1.length() - 1);
////                    }
////                    if (!attr1.trim().isEmpty()) {
////                        String[] tmp = attr1.trim().split("/");
////                        String tmp1 = "";
////                        for (int i = 0; i < tmp.length; i++) {
////                            tmp1 = tmp1 + "\t";
////                        }
////                        task.add(tmp1 + attr);
////                    }
//
//
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        for (String link : task) {
//            sb.append(link.toString() + "\n");
//        }
//        return sb.toString();
        return "";
    }

    private SiteEntity createSiteEntity(Site site) {
        String url = site.getUrl();
        SiteEntity indexingSite = new SiteEntity();
        indexingSite.setStatus(Status.INDEXING);
        indexingSite.setStatusTime(LocalDateTime.now());
        indexingSite.setLastError("TEST MESSAGE");
        indexingSite.setUrl(site.getUrl());
        indexingSite.setName(site.getName());

        Document document;
        Elements elements;
        Set<PageEntity> pages = new HashSet<>();
        try {
            document = Jsoup.connect(url)
                    .userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) " +
                            "Gecko/20070725 Firefox/2.0.0.6").referrer("http://www.google.com").get();
            elements = document.select("a");
            for (Element element : elements) {
                String attr = element.attr("abs:href");
                if (!attr.isEmpty() && attr.startsWith(url) && !links.contains(attr) && !attr.contains("#")) {
                    String attr1 = attr.replace(url, "");

                    String fullPath = url;
                    PageEntity indexingPage = new PageEntity();
                    indexingPage.setSiteId(indexingSite);
                    indexingPage.setPath(attr1);
                    URL url1;
                    int code;
                    try {
                        indexingPage.setContent(Jsoup.connect(fullPath).get().html());
                        url1 = new URL(attr);
                        HttpURLConnection connection = (HttpURLConnection) url1.openConnection();
                        connection.setRequestMethod("GET");
                        connection.connect();
                        code = connection.getResponseCode();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    indexingPage.setCode(code);
                    pageRepository.save(indexingPage);
                    pages.add(indexingPage);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        indexingSite.setPage(pages);

        return indexingSite;
    }

    private Set<PageEntity> createPage(Site site) {
        HashSet<PageEntity> pages = new HashSet<>();
        String url = site.getUrl();

        return pages;
    }
}
