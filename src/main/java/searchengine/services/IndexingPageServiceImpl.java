package searchengine.services;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import searchengine.dto.db.DbCommands;
import searchengine.dto.indexing.Lemmatizator;
import searchengine.model.*;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
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

    private final DbCommands dbCommands = new DbCommands();

    private Lemmatizator lemmatizator;

    private final Pattern siteLinkPattern = Pattern.compile("https?:\\/\\/[a-z-0-9.]+\\.[a-z]{2,3}");

    @Override
    public Map<String, String> indexPage(String url) {
        long start = System.currentTimeMillis();
        System.out.println("Start indexing new page: " + url);
        Map<String, String> result = new HashMap<>();
        if (url.isEmpty()) {
            result.put("result", "false");
            System.out.println("Индексация страницы завершена за " + (System.currentTimeMillis() - start) + " миллисекунд");
            return result;
        }

        try (java.sql.Connection connection = dbCommands.getNewConnection()) {
            String sqlSelect = "SELECT * FROM page WHERE path='" + url + "'";
            ResultSet rs = connection.createStatement().executeQuery(sqlSelect);
            if (!rs.next()) {
                Connection.Response response;

                Matcher matcher = siteLinkPattern.matcher(url);
                String rootUrl = "";
                if (matcher.find()) {
                    rootUrl = matcher.group();
                }

                String selectLike = "SELECT * FROM page WHERE path LIKE '" + rootUrl + "%' LIMIT 1";
                ResultSet pageRs = connection.createStatement().executeQuery(selectLike);
                int siteId;
                if (pageRs.next()) {
                    siteId = pageRs.getInt("site_id_id");

                    response = Jsoup.connect(url)
                            .userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) " +
                                    "Gecko/20070725 Firefox/2.0.0.6")
                            .timeout(10000)
                            .execute();

                    lemmatizator = Lemmatizator.getInstance();

                    PageEntity indexingPage = new PageEntity();
                    SiteEntity newSite = new SiteEntity();
                    newSite.setId(siteId);
                    indexingPage.setSiteId(newSite);
                    indexingPage.setCode(response.statusCode());
                    indexingPage.setContent(response.parse().html());
                    indexingPage.setPath(url);
                    pageRepository.save(indexingPage);

                    //Получаем HTML страницы и убираем разметку
                    String pageText = Jsoup.parse(indexingPage.getContent()).text();
                    //Получаем леммы и их частоту
                    Map<String, Integer> lemmas = lemmatizator.collectLemmas(pageText);

                    LemmaEntity lemmaEntity = new LemmaEntity();
                    IndexEntity indexEntity = new IndexEntity();

                    //Пишем полученные значения в таблицы lemma и index_table
                    writeLemmas(connection, lemmas, lemmaEntity, indexEntity, indexingPage, siteId, newSite);
                } else {
                    // Если адрес сайта отличается от переданной страницы, возвращаем ошибку
                    result.put("result", "false");
                    result.put("error", "Данная страница находится за пределами сайтов, указанных в конфигурационном файле");
                    System.out.println("Индексация страницы завершена за " + (System.currentTimeMillis() - start) + " миллисекунд");
                    return result;
                }
            } else {
                // Если переданная страница имеется в таблице page
                // получаем id страницы
                int pageId = rs.getInt("id");

                // Получаем все записи из index_table по полученному id
                String indexSelect = "SELECT * FROM index_table WHERE page_id_id ='" + pageId + "'";
                ResultSet indexRs = connection.createStatement().executeQuery(indexSelect);
                while (indexRs.next()) {
                    String lemmaId = indexRs.getString("lemma_id_id");
                    String lemmaSelect = "SELECT * FROM lemma WHERE id ='" + lemmaId + "'";
                    ResultSet lemmaRs = connection.createStatement().executeQuery(lemmaSelect);
                    if (lemmaRs.next()) {
                        int currentValue = lemmaRs.getInt("frequency");
                        // Если значение леммы больше 1, понижаем значение. Если 1 и менее -> удаляем запись из таблицы lemma
                        if (currentValue > 1) {
                            dbCommands.updateDbData("lemma", "frequency", String.valueOf(--currentValue),
                                    "id", lemmaId);
                        } else {
                            dbCommands.deleteFromDb("lemma", "id", lemmaId);
                        }
                    }
                }
                // Удаляем записи из таблицы index_table
                dbCommands.deleteFromDb("index_table", "page_id_id", String.valueOf(pageId));

                // Удаляем записи из таблицы page
                dbCommands.deleteFromDb("page", "id", String.valueOf(pageId));
                indexPage(url);
            }
            result.put("result", "true");
            System.out.println("Индексация страницы завершена за " + (System.currentTimeMillis() - start) + " миллисекунд");
            return result;
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeLemmas(java.sql.Connection connection, Map<String, Integer> lemmas, LemmaEntity lemmaEntity,
                             IndexEntity indexEntity, PageEntity indexingPage, int siteId,
                             SiteEntity newSite) throws SQLException {

        for (var entry : lemmas.entrySet()) {
            String sqlSelect = "SELECT * FROM lemma WHERE lemma ='" + entry.getKey() + "' AND site_id_id = '" + siteId + "'";
            ResultSet rs = connection.createStatement().executeQuery(sqlSelect);
            if (rs.next()) {
                int currentValue = rs.getInt("frequency");
                String updateQuery = "UPDATE lemma SET frequency='" + ++currentValue + "' WHERE site_id_id='" +
                        siteId + "' AND lemma = '" + entry.getKey() + "'";
                connection.createStatement().executeUpdate(updateQuery);
                indexEntity.setRankValue(entry.getValue());
                LemmaEntity newLemma = new LemmaEntity();
                newLemma.setId(rs.getInt("id"));
                indexEntity.setLemmaId(newLemma);
                indexEntity.setPageId(indexingPage);
                indexRepository.save(indexEntity);
            } else {
                lemmaEntity.setLemma(entry.getKey());
                lemmaEntity.setFrequency(1);
                lemmaEntity.setSiteId(newSite);
                lemmaRepository.save(lemmaEntity);

                indexEntity.setRankValue(entry.getValue());
                indexEntity.setLemmaId(lemmaEntity);
                indexEntity.setPageId(indexingPage);
                indexRepository.save(indexEntity);

                lemmaEntity = new LemmaEntity();
            }
            indexEntity = new IndexEntity();
        }
    }

}
