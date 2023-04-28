package searchengine.dto.indexing;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import searchengine.config.Site;
import searchengine.dto.errors.IndexingError;
import searchengine.model.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.regex.Pattern;


public class SiteIndexer extends RecursiveAction {
    private final List<Site> sites;
    private final List<SiteIndexer> siteIndexerList = new ArrayList<>();

    private final SiteRepository siteRepository;
    private final PageRepository pageRepository;

    public SiteIndexer(List<Site> sites, SiteRepository siteRepository, PageRepository pageRepository) {
        this.sites = sites;
        this.siteRepository = siteRepository;
        this.pageRepository = pageRepository;
    }

    @Override
    protected void compute() {

        //Обходим список сайтов, удаляя пройденные создаем и разветвляем на форки SiteIndexerы
        Iterator<Site> iterator = sites.iterator();

        while (iterator.hasNext()) {
            Site currentSite = iterator.next();
            iterator.remove();
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
        String name = site.getName();
        String emptyStringError = "";
        PageIndexer pageIndexer = new PageIndexer(Status.INDEXING, LocalDateTime.now(),
                emptyStringError, url, name);
        pageIndexer.parseSite();
    }

    private class PageIndexer {

        private final Status status;
        private final LocalDateTime statusTime;
        private final String lastError;
        private final String siteUrl;
        private final String name;
        private final Set<PageEntity> pages = new HashSet<>();

        private PageIndexer(Status status, LocalDateTime statusTime, String lastError,
                            String siteUrl, String name) {
//            this.indexingSite = indexingSite;
            this.status = status;
            this.statusTime = statusTime;
            this.lastError = lastError;
            this.siteUrl = siteUrl;
            this.name = name;
        }

        private SiteEntity parseSite() {
            SiteEntity indexingSite = new SiteEntity();
            indexingSite.setStatus(status);
            indexingSite.setStatusTime(statusTime);
            indexingSite.setLastError("");
            indexingSite.setUrl(siteUrl);
            indexingSite.setName(name);


            Pattern pattern = Pattern.compile("\\.pdf$");
            Document document;

            try {
                Connection connection = Jsoup.connect(siteUrl);
                connection.userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) " +
                        "Gecko/20070725 Firefox/2.0.0.6");
                connection.referrer("http://www.google.com");
                document = connection.get();

                Elements elements = document.select("a");
                Set<PageEntity> pages = new HashSet<>();
                Set<String> links = new HashSet<>();

                for (Element element : elements) {
                    String attr = element.attr("abs:href");

                    if (!attr.isEmpty() && attr.startsWith(siteUrl) && !attr.contains("#")) {
                        try {
                            Connection.Response response = Jsoup.connect(attr)
                                    .userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) " +
                                            "Gecko/20070725 Firefox/2.0.0.6")
                                    .timeout(10000)
                                    .execute();

                            String attr1 = attr.replace(siteUrl, "");
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
                            if (pattern.matcher(attr).find()) {
                                indexingSite.setStatus(Status.FAILED);
                                indexingSite.setLastError(e.getMessage());
                                indexingSite.setPage(null);
                                siteRepository.save(indexingSite);
                                System.out.println("END " + Thread.currentThread().getName() + "\n");
                                return indexingSite;
                            } else throw new RuntimeException(e);
                        }
                    }
                }

                indexingSite.setStatus(Status.INDEXED);

                siteRepository.save(indexingSite);

                pages.forEach(pageRepository::save);

                System.out.println("END " + Thread.currentThread().getName() + "\n");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return indexingSite;
        }
    }
}
