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
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.regex.Pattern;


public class SiteIndexer extends RecursiveAction {
    private final List<Site> sites;
    private final List<SiteIndexer> siteIndexerList = new ArrayList<>();
    private final SiteRepository siteRepository;
    private final PageRepository pageRepository;


    private final LemmaRepository lemmaRepository;


    private final IndexRepository indexRepository;

    private final boolean isStoped;

    public SiteIndexer(List<Site> sites, SiteRepository siteRepository, PageRepository pageRepository, LemmaRepository lemmaRepository, IndexRepository indexRepository, boolean isStoped) {
        this.sites = sites;
        this.siteRepository = siteRepository;
        this.pageRepository = pageRepository;
        this.lemmaRepository = lemmaRepository;
        this.indexRepository = indexRepository;
        this.isStoped = isStoped;
    }

    @Override
    protected void compute() {
        //Обходим список сайтов, удаляя пройденные создаем и разветвляем на форки SiteIndexerы
        Iterator<Site> iterator = sites.iterator();

        while (iterator.hasNext()) {
            Site currentSite = iterator.next();
            iterator.remove();
            SiteIndexer newIndexer = new SiteIndexer(sites, siteRepository, pageRepository, lemmaRepository, indexRepository, isStoped);
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
        private final Statement stmt;

        private Lemmatizator lemmatizator;

        // Подключение к БД с целью получения Statement stmt
        {
            try {
                lemmatizator = Lemmatizator.getInstance();
                java.sql.Connection connect = DriverManager.
                        getConnection("jdbc:mysql://localhost/search_engine", "root", "pass");
                stmt = connect.createStatement();
            } catch (SQLException | IOException e) {
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
                List<LemmaEntity> lemmaEntities = new ArrayList<>();
                List<IndexEntity> indexEntities = new ArrayList<>();

                // Обновляем дочерние ссылки
                pageLinksCrawling(pages, indexingSite, lemmaEntities, indexEntities, siteUrlWithHttps.toString());

                // Проверка на ошибку индексации
                if (indexingSite.getLastError().isEmpty()) {
                    pages.forEach(pageRepository::save);
                }

                lemmaEntities.forEach(lemmaRepository::save);
                indexEntities.forEach(indexRepository::save);

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
        private SiteEntity pageLinksCrawling(Set<PageEntity> pages, SiteEntity indexingSite,
                                             List<LemmaEntity> lemmaEntities, List<IndexEntity> indexEntities,
                                             String siteUrlWithHttps) {
            Connection.Response response;

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

                    String trimLink = trimLink(attr);

                    if (isRepeatLink("page", "path", trimLink)) {
                        continue;
                    }

                    if (!attr.isEmpty() && attr.startsWith(siteUrl) || attr.startsWith(siteUrlWithHttps.toString()) && !attr.contains("#")) {
                        try {
                            response = Jsoup.connect(attr)
                                    .userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) " +
                                            "Gecko/20070725 Firefox/2.0.0.6")
                                    .timeout(10000)
                                    .execute();

                            if (links.add(trimLink)) {
                                PageEntity indexingPage = new PageEntity();
                                indexingPage.setSiteId(indexingSite);
                                indexingPage.setPath(trimLink);
                                indexingPage.setContent(response.parse().html());
                                indexingPage.setCode(response.statusCode());
                                pages.add(indexingPage);

                                //Получаем код страницы
                                String pageContent = indexingPage.getContent();
                                //Убираем html разметку
                                String pageText = Jsoup.parse(pageContent).text();
                                //Получаем леммы и их частоту
                                Map<String, Integer> lemmas = lemmatizator.collectLemmas(pageText);

                                //Пишем полученные значения в таблицы lemma и index_table
                                LemmaEntity lemmaEntity = new LemmaEntity();
                                IndexEntity indexEntity = new IndexEntity();

                                for (var entry : lemmas.entrySet()) {
                                    ResultSet resultFromLemma = selectFromDb(entry.getKey(), "lemma", "lemma");
                                    // Проверка на отсутствие леммы в таблице lemma, а так же на разницу site_id
                                    if (!resultFromLemma.next() ||
                                            (resultFromLemma.next() && resultFromLemma.getInt("site_id_id") != indexingSite.getId())) {
                                        lemmaEntity.setLemma(entry.getKey());
                                        lemmaEntity.setFrequency(1);
                                        lemmaEntity.setSiteId(indexingSite);
                                        lemmaEntities.add(lemmaEntity);

                                        indexEntity.setRankValue(entry.getValue());
                                        indexEntity.setLemmaId(lemmaEntity);
                                        indexEntity.setPageId(indexingPage);
                                        indexEntities.add(indexEntity);

                                        lemmaEntity = new LemmaEntity();
                                        indexEntity = new IndexEntity();
                                    } else if (resultFromLemma.next() && resultFromLemma.getInt("site_id_id") == indexingSite.getId()) {
                                        int currentValue = resultFromLemma.getInt("frequency");
                                        updateFrequencyData(entry.getKey(), currentValue);
                                    }
                                }
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
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
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
            Set<PageEntity> pages = Collections.synchronizedSet(new HashSet<>());
            List<LemmaEntity> lemmaEntities = new ArrayList<>();
            List<IndexEntity> indexEntities = new ArrayList<>();

            pageLinksCrawling(pages, indexingSite, lemmaEntities, indexEntities, siteUrlWithHttps.toString());

            if (indexingSite.getStatus() != Status.FAILED) {
                indexingSite.setStatus(Status.INDEXED);
            }

            siteRepository.save(indexingSite);

            if (indexingSite.getLastError().isEmpty()) {
                pages.forEach(pageRepository::save);

                LemmasWriter forkWriterLemmas = new LemmasWriter(lemmaEntities, lemmaRepository);
                IndexesWriter forkWriterIndexes = new IndexesWriter(indexEntities, indexRepository);
                ForkJoinPool forkJoinPool = new ForkJoinPool();
                forkJoinPool.invoke(forkWriterLemmas);
                forkJoinPool.invoke(forkWriterIndexes);
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

        private ResultSet selectFromDb(String data, String table, String column) {
            String sqlSelect = "SELECT * FROM " + table + " WHERE " + column + "='" + data + "'";
            try {
                return stmt.executeQuery(sqlSelect);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        private void updateFrequencyData(String lemma, int currentValue) {
            String sqlUpdate = "UPDATE lemma SET frequency='" + currentValue++ + "' WHERE lemma='" + lemma + "'";
            try {
                stmt.executeUpdate(sqlUpdate);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
