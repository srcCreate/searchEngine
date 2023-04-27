package searchengine.dto.indexing;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import searchengine.config.Site;
import searchengine.model.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;
import java.util.regex.Pattern;


public class SiteIndexer extends RecursiveAction {
    private List<Site> sites;
    private Set<PageEntity> pages = new HashSet<>();

    private List<SiteIndexer> siteIndexerList = new ArrayList<>();

    private SiteRepository siteRepository;
    private PageRepository pageRepository;

    public SiteIndexer(List<Site> sites, SiteRepository siteRepository, PageRepository pageRepository) {
        this.sites = sites;
        this.siteRepository = siteRepository;
        this.pageRepository = pageRepository;
    }

    @Override
    protected void compute() {

        //Обходим список сайтов, удаляя пройденные создаем и разветвляем на форки SiteIndexerы
        for (Site currentSite : sites) {

            sites.remove(currentSite);
            SiteIndexer newIndexer = new SiteIndexer(sites, siteRepository, pageRepository);
            siteIndexerList.add(newIndexer);
            newIndexer.fork();
            newIndexer.siteEntityCreator(currentSite);

        }
        //Объединяем каждый их SiteIndexеr
        siteIndexerList.forEach(ForkJoinTask::join);
    }

    private void siteEntityCreator(Site site) {
        System.out.println("Start " + Thread.currentThread().getName() + "\n");

        String url = site.getUrl();
        SiteEntity indexingSite = new SiteEntity();
        indexingSite.setStatus(Status.INDEXING);
        indexingSite.setStatusTime(LocalDateTime.now());
        indexingSite.setLastError("TEST MESSAGE");
        indexingSite.setUrl(url);
        indexingSite.setName(site.getName());

        PageIndexer pageIndexer = new PageIndexer(indexingSite, url);
        pages = pageIndexer.parseSite();

        System.out.println("END " + Thread.currentThread().getName() + "\n");

    }

    private class PageIndexer {
        private final SiteEntity indexingSite;
        private String url;

        private PageIndexer(SiteEntity indexingSite, String url) {
            this.indexingSite = indexingSite;
            this.url = url;
        }

        private Set<PageEntity> parseSite() {
            Pattern pattern = Pattern.compile("\\.pdf$");
            Document document;

            try {
                Connection connection = Jsoup.connect(url);
                connection.userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) " +
                        "Gecko/20070725 Firefox/2.0.0.6");
                connection.referrer("http://www.google.com");
                document = connection.get();

                Elements elements = document.select("a");
                Set<PageEntity> pages = new HashSet<>();
                Set<String> links = new HashSet<>();

                for (Element element : elements) {
                    String attr = element.attr("abs:href");

                    if (pattern.matcher(attr).find()) {
                        continue;
                    }

                    if (!attr.isEmpty() && attr.startsWith(url) && !attr.contains("#")) {
                        try {
                            Connection.Response response = Jsoup.connect(attr)
                                    .userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) " +
                                            "Gecko/20070725 Firefox/2.0.0.6")
                                    .timeout(10000)
                                    .execute();

                            String attr1 = attr.replace(url, "");
                            int lastChar = attr1.length() - 1;
                            if (!attr1.trim().isEmpty() && attr1.charAt(lastChar) == '/') {
                                attr1 = attr1.substring(0, lastChar);
                            }

                            if (links.add(attr1)) {
                                PageEntity indexingPage = new PageEntity();
                                indexingPage.setSiteId(indexingSite);
                                indexingPage.setPath(attr1);
                                indexingPage.setContent(response.parse().html());
                                indexingPage.setCode(response.statusCode());

                                pages.add(indexingPage);
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }

                siteRepository.save(indexingSite);

                pages.forEach(pageRepository::save);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return pages;
        }
    }
}
