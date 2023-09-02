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
import java.util.regex.Pattern;

public class PageIndexer {
    private SiteEntity site;
    private DbCommands dbCommands = new DbCommands();

    private PageRepository pageRepository;

    private LemmaRepository lemmaRepository;

    private IndexRepository indexRepository;

    private Lemmatizator lemmatizator;

    private Map<String, Integer> lemmasCounter = new HashMap<>();

    public PageIndexer(SiteEntity site, PageRepository pageRepository, LemmaRepository lemmaRepository,
                       IndexRepository indexRepository) {
        this.site = site;
        this.pageRepository = pageRepository;
        this.lemmaRepository = lemmaRepository;
        this.indexRepository = indexRepository;
    }

    public void parsePages() {
        String siteUrl = site.getUrl();
        StringBuilder siteUrlWithHttps = new StringBuilder(site.getUrl());
        if (siteUrl.charAt(4) != 's') {
            siteUrlWithHttps.insert(4, 's');
        }

        Connection.Response response;

        // Паттерн проверки ссылок на pdf документ
        Pattern pattern = Pattern.compile("\\.pdf$");
        Document document;

        // Сет исключающий дублирование ссылок
        Set<String> links = new HashSet<>();

        try {
            Connection connection = Jsoup.connect(site.getUrl());
            connection.userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) " +
                    "Gecko/20070725 Firefox/2.0.0.6");
            connection.referrer("http://www.google.com");
            document = connection.get();

            Elements elements = document.select("a");

            // Получаем экземпляр лемматизатора
            lemmatizator = Lemmatizator.getInstance();

            for (Element element : elements) {
                String attr = element.attr("abs:href");

                String trimLink = trimLink(attr);

                if (!attr.isEmpty() && attr.startsWith(site.getUrl()) ||
                        attr.startsWith(siteUrlWithHttps.toString()) &&
                                !attr.contains("#")) {
                    try {
                        response = Jsoup.connect(attr)
                                .userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) " +
                                        "Gecko/20070725 Firefox/2.0.0.6")
                                .timeout(10000)
                                .execute();

                        if (links.add(trimLink)) {
                            PageEntity indexingPage = new PageEntity();
                            indexingPage.setSiteId(site);
                            indexingPage.setPath(trimLink);
                            indexingPage.setContent(response.parse().html());
                            indexingPage.setCode(response.statusCode());
                            pageRepository.save(indexingPage);

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
                                if (lemmasCounter.containsKey(entry.getKey())) {
                                    String key = entry.getKey();
                                    ResultSet rs = dbCommands.selectFromDbWithTwoParameters("lemma",
                                            "lemma", "site_id_id", key,
                                            String.valueOf(site.getId()));
                                    if (rs.next()) {
                                        int lemmaId = rs.getInt("id");
                                        lemmasCounter.put(key, lemmasCounter.get(entry.getKey()) + 1);
                                        dbCommands.updateDbData("lemma","frequency",
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
                    } catch (IOException | SQLException e) {
                        site.setStatus(Status.FAILED);
                        if (pattern.matcher(attr).find()) {
                            site.setLastError(e.getMessage());
                            dbCommands.updateDbData("site", "last_error", "Link to PDF",
                                    "url", siteUrl);
                            System.out.println("END " + Thread.currentThread().getName() + "\n");
                            break;
                        }
                        site.setLastError(e.getMessage());
                        dbCommands.updateDbData("site", "last_error", e.getMessage(),
                                "url", siteUrl);
                        System.out.println("END " + Thread.currentThread().getName() + "\n");
                        break;
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String trimLink(String link) {
        link.replace(site.getUrl(), "");
        int lastChar = link.length() - 1;
        if (!link.trim().isEmpty() && link.charAt(lastChar) == '/') {
            link = link.substring(0, lastChar);
        }
        return link;
    }
}
