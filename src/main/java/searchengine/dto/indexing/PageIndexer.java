package searchengine.dto.indexing;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import searchengine.dto.db.DbCommands;
import searchengine.model.*;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.RecursiveAction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PageIndexer extends RecursiveAction {
    private final SiteEntity site;
    private final List<String> urls;
    private final Set<PageUrl> uniqueUrl;
    private final Map<String, Integer> lemmasCounter;
    private final SiteIndexer siteIndexer;
    private Lemmatizator lemmatizator;
    private final DbCommands dbCommands = new DbCommands();
    private final PageRepository pageRepository;
    private final LemmaRepository lemmaRepository;
    private final IndexRepository indexRepository;

    public PageIndexer(SiteEntity site, List<String> urls,
                       SiteIndexer siteIndexer) {
        this.site = site;
        this.urls = urls;
        this.siteIndexer = siteIndexer;
        this.uniqueUrl = siteIndexer.getUniqueUrl();
        this.lemmasCounter = siteIndexer.getLemmasCounter();
        this.pageRepository = siteIndexer.getPageRepository();
        this.lemmaRepository = siteIndexer.getLemmaRepository();
        this.indexRepository = siteIndexer.getIndexRepository();
    }

    @Override
    protected void compute() {

        List<PageIndexer> listPageIndexerTasks = getTasks();
        for (PageIndexer indexer : listPageIndexerTasks) {
            indexer.join();
        }
    }

    private List<PageIndexer> getTasks() {
        List<PageIndexer> tasks = new ArrayList<>();

        urls.forEach(url -> {
            PageUrl nextPageUrl = new PageUrl(url);
            if (!uniqueUrl.contains(nextPageUrl)) {
                List<String> urlsToParse = getUrlToParse(url);
                PageIndexer nextPageIndexer = new PageIndexer(site, urlsToParse, siteIndexer);
                uniqueUrl.add(nextPageUrl);
                nextPageIndexer.fork();
                tasks.add(nextPageIndexer);
                nextPageIndexer.createPageEntity(url);
            }
        });
        return tasks;
    }

    private void createPageEntity(String url) {
        try (java.sql.Connection connection = dbCommands.getNewConnection()) {
            Connection.Response response;
            Pattern pattern = Pattern.compile("\\.pdf$");

            try {
                lemmatizator = Lemmatizator.getInstance();
                response = Jsoup.connect(url)
                        .userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) " +
                                "Gecko/20070725 Firefox/2.0.0.6")
                        .timeout(10000)
                        .ignoreHttpErrors(true)
                        .execute();


                PageEntity indexingPage = new PageEntity();
                indexingPage.setSiteId(site);
                indexingPage.setPath(url);
                indexingPage.setContent(response.parse().html());
                int statusCode = response.statusCode();
                indexingPage.setCode(statusCode);
                if (statusCode == 200) {
                    pageRepository.save(indexingPage);

                    //Получаем код страницы и убираем html разметку
                    String pageText = Jsoup.parse(indexingPage.getContent()).text();
                    //Получаем леммы и их частоту
                    Map<String, Integer> lemmas = lemmatizator.collectLemmas(pageText);

                    LemmaEntity lemmaEntity = new LemmaEntity();
                    IndexEntity indexEntity = new IndexEntity();

                    //Пишем полученные значения в таблицы lemma и index_table
                    writeLemmas(connection, lemmas, lemmaEntity, indexEntity, indexingPage);
                }
            } catch (IOException | SQLException e) {
                site.setStatus(Status.FAILED);
                if (pattern.matcher(url).find()) {
                    site.setLastError(e.getMessage());
                    dbCommands.updateDbData("site", "last_error", "Link to PDF",
                            "url", url);
                    System.out.println("END " + Thread.currentThread().getName() + "\n");
                }
                site.setLastError(e.getMessage());
                dbCommands.updateDbData("site", "last_error", e.getMessage(),
                        "url", url);
                System.out.println("END " + Thread.currentThread().getName() + "\n");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


    }

    private List<String> getUrlToParse(String url) {
        List<String> urls = new ArrayList<>();
        Elements linkElements = getLinkElements(url);

        for (Element element : linkElements) {
            String correctLink = element.attr("abs:href");
            if (checkUrl(correctLink)) {
                urls.add(correctLink);
            }
        }
        return urls;
    }

    private synchronized boolean checkUrl(String url) {
        String mediaRegex = "(https?:\\/\\/.*\\.(?:png|jpg|gif|bmp|jpeg|PNG|JPG|GIF|BMP|pdf|php|zip))$|[?|#]";
        Matcher matcher = Pattern.compile(mediaRegex).matcher(url);
        if (matcher.find()) {
            return false;
        }
        return !url.isEmpty() && url.startsWith(site.getUrl())
                && !url.contains("#") && !uniqueUrl.contains(url);
    }

    private Elements getLinkElements(String url) {
        Connection connection = Jsoup.connect(url);
        Document document;
        connection.ignoreHttpErrors(true);
        connection.userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) " +
                "Gecko/20070725 Firefox/2.0.0.6");
        connection.referrer("http://www.google.com");
        try {
            document = connection.get();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return document.select("a");
    }

    private void writeLemmas(java.sql.Connection connection, Map<String, Integer> lemmas, LemmaEntity lemmaEntity,
                             IndexEntity indexEntity, PageEntity indexingPage) throws SQLException {

        for (var entry : lemmas.entrySet()) {
            if (lemmasCounter.containsKey(entry.getKey())) {
                String key = entry.getKey();
                String sqlSelect = "SELECT * FROM lemma WHERE lemma ='" + key + "' and site_id_id ='" + site.getId() + "'";

                ResultSet rs = connection.createStatement().executeQuery(sqlSelect);
                if (rs.next()) {
                    int lemmaId = rs.getInt("id");
                    lemmasCounter.put(key, lemmasCounter.get(entry.getKey()) + 1);
                    dbCommands.updateDbData("lemma", "frequency",
                            String.valueOf(lemmasCounter.get(key)), "id",
                            String.valueOf(lemmaId));

                    LemmaEntity newLemma = new LemmaEntity();
                    newLemma.setId(lemmaId);

                    indexEntity.setRankValue(entry.getValue());
                    indexEntity.setLemmaId(newLemma);
                    indexEntity.setPageId(indexingPage);
                    indexRepository.save(indexEntity);

                    lemmaEntity = new LemmaEntity();
                    indexEntity = new IndexEntity();
                }
            } else {
                lemmaEntity.setLemma(entry.getKey());
                lemmaEntity.setFrequency(1);
                lemmaEntity.setSiteId(site);
                lemmasCounter.put(entry.getKey(), lemmaEntity.getFrequency());
                lemmaRepository.save(lemmaEntity);

                indexEntity.setRankValue(entry.getValue());
                indexEntity.setLemmaId(lemmaEntity);
                indexEntity.setPageId(indexingPage);
                indexRepository.save(indexEntity);

                lemmaEntity = new LemmaEntity();
                indexEntity = new IndexEntity();
            }
        }
    }
}
