package searchengine.services;

import lombok.RequiredArgsConstructor;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Service;
import searchengine.config.Site;
import searchengine.config.SitesList;
import searchengine.dto.indexing.PageIndexer;
import searchengine.model.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

@Service
@RequiredArgsConstructor
public class IndexingServiceImpl implements IndexingService{

    private static CopyOnWriteArrayList<String> links = new CopyOnWriteArrayList<>();
    private final SitesList sites;

    private final SiteRepository siteRepository;
    private final PageRepository pageRepository;


    // Реализован метод, но пока что без многопоточности. Сначала хочу разобраться как будет происходить работа метода.
    // позже добавлю потоки.
    @Override
    public PageIndexer startIndexing() {
        int numFlow = Runtime.getRuntime().availableProcessors() - 1;

        long start = System.currentTimeMillis();
        parse();
        System.out.println("Индексация сайтов завершена за " + (System.currentTimeMillis() - start) + " миллисекунд");

        return null;
    }

    private void parse() {
        List<Site> sitesList = sites.getSites();
        for (Site site : sitesList) {
            SiteEntity indexingSite = createSiteEntity(site);
            siteRepository.save(indexingSite);
        }
    }

    private SiteEntity createSiteEntity(Site site) {
        String url = site.getUrl();
        SiteEntity indexingSite = new SiteEntity();
        indexingSite.setStatus(Status.INDEXING);
        indexingSite.setStatusTime(LocalDateTime.now());
        indexingSite.setLastError("TEST MESSAGE");
        indexingSite.setUrl(site.getUrl());
        indexingSite.setName(site.getName());

        Connection.Response response;
        Document document;
        try {
//            response = (Connection.Response) Jsoup.connect(url)
//                    .userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) " +
//                            "Gecko/20070725 Firefox/2.0.0.6").get();
            response = Jsoup.connect(url)
                    .userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) " +
                            "Gecko/20070725 Firefox/2.0.0.6")
                    .timeout(10000)
                    .execute();
            document = response.parse();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Elements elements = document.select("a");
        Set<PageEntity> pages = new HashSet<>();
        for (Element element : elements) {
            String attr = element.attr("abs:href");
            if (!attr.isEmpty() && attr.startsWith(url) && !links.contains(attr) && !attr.contains("#")) {
                try {
                    response = Jsoup.connect(attr)
                            .userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) " +
                                    "Gecko/20070725 Firefox/2.0.0.6")
                            .timeout(10000)
                            .execute();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                String attr1 = attr.replace(url, "");

                PageEntity indexingPage = new PageEntity();
                indexingPage.setSiteId(indexingSite);
                indexingPage.setPath(attr1);
                indexingPage.setContent(document.html());
                indexingPage.setCode(response.statusCode());

                pageRepository.save(indexingPage);
                pages.add(indexingPage);
            }
        }

        indexingSite.setPage(pages);

        return indexingSite;
    }
}
