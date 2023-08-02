package searchengine.services;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import searchengine.dto.indexing.Lemmatizator;
import searchengine.model.*;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class IndexingPageServiceImpl implements IndexingPageService {

    @Autowired
    private PageRepository pageRepository;

    @Autowired
    private LemmaRepository lemmaRepository;

    @Autowired
    private IndexRepository indexRepository;

    private PageEntity page = new PageEntity();

    private Statement stmt;

    private Lemmatizator lemmatizator;

    {
        try {
            lemmatizator = Lemmatizator.getInstance();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Pattern pattern = Pattern.compile("https?:\\/\\/[a-z-0-9.]+\\.[a-z]{2,3}");

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

    @Override
    public Map<String, String> indexPage(String url) {
        Map<String, String> result = new HashMap<>();
        if (url.isEmpty()) {
            result.put("result", "false");
            return result;
        }

        try {
            if (!selectFromDb(url, "page", "path").next()) {
                Connection.Response response;

                try {
                    ResultSet resultSet = searchUrlRootLike(url, "page", "path");
                    int siteId = 0;
                    if (resultSet.next()) {
                        siteId = resultSet.getInt("site_id_id");
                    } else {
                        result.put("result", "false");
                        result.put("error", "Данная страница находится за пределами сайтов, указанных в конфигурационном файле");
                        return result;
                    }

                    response = Jsoup.connect(url)
                            .userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) " +
                                    "Gecko/20070725 Firefox/2.0.0.6")
                            .timeout(10000)
                            .execute();

                    PageEntity indexingPage = new PageEntity();
                    resultSet = selectFromDb(String.valueOf(siteId), "site", "id");
                    SiteEntity newSite = new SiteEntity();
                    if (resultSet.next()) {
                        newSite.setId(resultSet.getInt("id"));
                    }
                    indexingPage.setSiteId(newSite);
                    indexingPage.setCode(response.statusCode());
                    indexingPage.setContent(response.parse().html());
                    indexingPage.setPath(url);
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
                        ResultSet resultFromLemma = selectFromDb(entry.getKey(), "lemma", "lemma");
                        // Проверка на отсутствие леммы в таблице lemma, а так же на разницу site_id
                        if (!resultFromLemma.next() ||
                                (resultFromLemma.next() && resultFromLemma.getInt("site_id_id") != newSite.getId())) {
                            lemmaEntity.setLemma(entry.getKey());
                            lemmaEntity.setFrequency(1);
                            lemmaEntity.setSiteId(newSite);
                            lemmaRepository.save(lemmaEntity);

                            indexEntity.setRankValue(entry.getValue());
                            indexEntity.setLemmaId(lemmaEntity);
                            indexEntity.setPageId(indexingPage);
                            indexRepository.save(indexEntity);

                            lemmaEntity = new LemmaEntity();
                            indexEntity = new IndexEntity();
                        } else if (resultFromLemma.next() && resultFromLemma.getInt("site_id_id") == newSite.getId()) {
                            int currentValue = resultFromLemma.getInt("frequency");
                            updateFrequencyData(entry.getKey(), currentValue);
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                // Получаем id страницы
                ResultSet resultSet = selectFromDb(url, "page", "path");
                int pageId = 0;
                if (resultSet.next()) {
                    pageId = resultSet.getInt("id");
                }
                // Получаем все записи из index_table по полученному id
                resultSet = selectFromDb(String.valueOf(pageId), "index_table","page_id_id");
                // Проходим по всем записям в таблицах и удаляем их
                while (resultSet.next()) {
                    int lemmaId = resultSet.getInt("lemma_id_id");
                    System.out.println("Lemma ID = " + lemmaId);
                    int indexId = resultSet.getInt("id");
                    System.out.println("Index ID = " + indexId);
                    deleteFromDb(String.valueOf(lemmaId), "lemma", "id");
                    deleteFromDb(String.valueOf(indexId), "index_table", "id");
                }
                deleteFromDb(String.valueOf(pageId), "page", "id");
                indexPage(url);
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        result.put("result", "true");
        return result;
    }

    private ResultSet selectFromDb(String data, String table, String column) {
        String sqlSelect = "SELECT * FROM " + table + " WHERE " + column + "='" + data + "'";
        try {
            return stmt.executeQuery(sqlSelect);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private ResultSet searchUrlRootLike(String url, String table, String column) {
        Matcher matcher = pattern.matcher(url);
        String rootUrl = "";
        if (matcher.find()) {
            rootUrl = matcher.group();
        }
        String sqlSelect = "SELECT * FROM " + table + " WHERE " + column + " LIKE '" + rootUrl + "%' LIMIT 1";
        try {
            return stmt.executeQuery(sqlSelect);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private int deleteFromDb(String data, String table, String column) {
        java.sql.Connection connect = null;
        Statement statement;
        String sqlSelect = "DELETE FROM " + table + " WHERE " + column + "='" + data + "'";

        try {
            connect = DriverManager.
                    getConnection("jdbc:mysql://localhost/search_engine", "root", "pass");
            statement = connect.createStatement();
            int count = statement.executeUpdate(sqlSelect);
            return count;
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

