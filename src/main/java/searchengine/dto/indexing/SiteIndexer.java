package searchengine.dto.indexing;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import searchengine.config.Site;
import searchengine.model.*;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.RecursiveAction;
import java.util.regex.Pattern;


public class SiteIndexer extends RecursiveAction {
    private final List<Site> sites;
    private final List<SiteIndexer> siteIndexerList = new ArrayList<>();
    private final SiteRepository siteRepository;
    private final PageRepository pageRepository;

    private boolean isStoped = false;

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
        for (SiteIndexer indexer : siteIndexerList) {
            if (!isStoped) {
                indexer.join();
            } else {
                System.out.println("STOP FROM COMPUTE");
                break;
            }
        }
//        siteIndexerList.forEach(ForkJoinTask::join);
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

    public void setStoped() {
        isStoped = true;
    }

    private class PageIndexer {

        private final Status status;
        private final LocalDateTime statusTime;
        private final String lastError;
        private final String siteUrl;
        private final String name;
        private final Statement stmt;

        // Подключение к БД с целью получения Statement stmt
        {
            try {
                java.sql.Connection connect = DriverManager.
                        getConnection("jdbc:mysql://localhost/search_engine", "root", "pass");
                stmt = connect.createStatement();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        private PageIndexer(Status status, LocalDateTime statusTime, String lastError,
                            String siteUrl, String name) {
            this.status = status;
            this.statusTime = statusTime;
            this.lastError = lastError;
            this.siteUrl = siteUrl;
            this.name = name;
        }

        //Метод обхода сайта с дочерними ссылками
        private SiteEntity parseSite() {
            SiteEntity indexingSite = new SiteEntity();
            StringBuilder siteUrlWithHttps = new StringBuilder(siteUrl);
            siteUrlWithHttps.insert(4, 's');

            // Если сайт проиндексирован ранее, обновляем время индексации и обходим повторно дочерние ссылки
            if (isRepeatLink("site", "url", siteUrl)) {

                // Обновляем время индексации
                updateSiteTableLink(siteUrl);

                String sqlQuery = "SELECT * FROM site WHERE url='" + siteUrl + "'";
                int id = 0;

                // получаем id имеющегося в бд сайта и задаем его для созданной сущности SiteEntity
                try {
                    ResultSet resultSet = stmt.executeQuery(sqlQuery);
                    if (resultSet.next()) {
                        id = resultSet.getInt("id");
                        indexingSite.setId(id);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

                // Создаем Set для будущих дочерних ссылок
                Set<PageEntity> pages = new HashSet<>();

                // Обновляем дочерние ссылки
                linkCrawling(pages, indexingSite, siteUrlWithHttps.toString());

                // Проверка на ошибку индексации
                if (indexingSite.getLastError().isEmpty()) {
                    pages.forEach(pageRepository::save);
                }

                System.out.println("END parsing " + siteUrl);
                System.out.println("END " + Thread.currentThread().getName() + "\n");
                return indexingSite;
            }

            indexingSite.setStatus(status);
            indexingSite.setStatusTime(statusTime);
            indexingSite.setLastError("");
            indexingSite.setUrl(siteUrl);
            indexingSite.setName(name);

            // индексация дочерних ссылок если сайт ранее не индексировался
            return parseChildLinks(siteUrl, indexingSite);
        }

        // Метод индексации дочерних ссылок
        private SiteEntity linkCrawling(Set<PageEntity> pages, SiteEntity indexingSite, String siteUrlWithHttps) {
            Connection.Response response = null;

            // Паттерн проверки ссылок на pdf документ
            Pattern pattern = Pattern.compile("\\.pdf$");
            Document document;

            Set<String> links = new HashSet<>();

            try {
                Connection connection = Jsoup.connect(siteUrl);
                connection.userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) " +
                        "Gecko/20070725 Firefox/2.0.0.6");
                connection.referrer("http://www.google.com");
                document = connection.get();

                Elements elements = document.select("a");

                for (Element element : elements) {
                    String attr = element.attr("abs:href");

                    String attr1 = trimLink(attr);

                    if (isRepeatLink("page", "path", attr1)) {
                        continue;
                    }

                    if (!attr.isEmpty() && attr.startsWith(siteUrl) || attr.startsWith(siteUrlWithHttps.toString()) && !attr.contains("#")) {
                        try {
                            response = Jsoup.connect(attr)
                                    .userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) " +
                                            "Gecko/20070725 Firefox/2.0.0.6")
                                    .timeout(10000)
                                    .execute();

                            if (links.add(attr1)) {
                                PageEntity indexingPage = new PageEntity();
                                indexingPage.setSiteId(indexingSite);
                                indexingPage.setPath(attr1);
                                indexingPage.setContent(response.parse().html());
                                indexingPage.setCode(response.statusCode());
                                pages.add(indexingPage);
                            }
                        } catch (IOException e) {
                            indexingSite.setStatus(Status.FAILED);
                            if (pattern.matcher(attr).find()) {
                                indexingSite.setLastError(e.getMessage());
                                System.out.println("END " + Thread.currentThread().getName() + "\n");
                                return indexingSite;
                            }
                            indexingSite.setLastError(e.getMessage());
                            indexingSite.setPage(null);
                            System.out.println("END " + Thread.currentThread().getName() + "\n");
                            return indexingSite;

//                            indexingSite.setStatus(Status.FAILED);
//                            System.out.println(e.getMessage());
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            indexingSite.setLastError("");
            return indexingSite;
        }

        private SiteEntity parseChildLinks(String siteUrl, SiteEntity indexingSite) {
            StringBuilder siteUrlWithHttps = new StringBuilder(siteUrl);
            siteUrlWithHttps.insert(4, 's');
            Set<PageEntity> pages = new HashSet<>();

            linkCrawling(pages, indexingSite, siteUrlWithHttps.toString());

            indexingSite.setStatus(Status.INDEXED);

            siteRepository.save(indexingSite);

            if (indexingSite.getLastError().isEmpty()) {
                pages.forEach(pageRepository::save);
            }

            System.out.println("END parsing " + siteUrl);
            System.out.println("END " + Thread.currentThread().getName() + "\n");

            return indexingSite;
        }

        private void updateSiteTableLink(String url) {
            String sqlUpdate = "UPDATE site SET status_time='" + LocalDateTime.now() + "' WHERE url='" + url + "'";
            try {
                stmt.executeUpdate(sqlUpdate);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        private String trimLink(String link) {
            link.replace(siteUrl, "");
            int lastChar = link.length() - 1;
            if (!link.trim().isEmpty() && link.charAt(lastChar) == '/') {
                link = link.substring(0, lastChar);
            }
            return link;
        }

        private boolean isRepeatLink(String tableName, String columnName, String link) {
            try {
                String sqlSelect = "SELECT * FROM " + tableName + " WHERE " + columnName + "='" + link + "'";
                ResultSet rs = stmt.executeQuery(sqlSelect);
                return rs.next();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
