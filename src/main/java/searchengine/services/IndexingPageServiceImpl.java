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

    private Pattern siteLinkPattern = Pattern.compile("https?:\\/\\/[a-z-0-9.]+\\.[a-z]{2,3}");


    @Override
    public Map<String, String> indexPage(String url) {
        Map<String, String> result = new HashMap<>();
        if (url.isEmpty()) {
            result.put("result", "false");
            return result;
        }

        try {
            if (!dbCommands.selectAllFromDb("page", "path", url).next()) {
                Connection.Response response;

                ResultSet resultSet = dbCommands.selectUrlLikeFromDB(siteLinkPattern, url, "page", "path");
                int siteId;
                if (resultSet.next()) {
                    siteId = resultSet.getInt("site_id_id");

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

                    //Получаем HTML страницы
                    String pageContent = indexingPage.getContent();
                    //Убираем HTML разметку
                    String pageText = Jsoup.parse(pageContent).text();
                    //Получаем леммы и их частоту
                    Map<String, Integer> lemmas = lemmatizator.collectLemmas(pageText);

                    LemmaEntity lemmaEntity = new LemmaEntity();
                    IndexEntity indexEntity = new IndexEntity();

                    for (var entry : lemmas.entrySet()) {
                        ResultSet resultFromLemma = dbCommands.selectAllFromDb("lemma", "lemma", entry.getKey());
                        // Если лемма существует в таблице lemma, то увеличиваем значение frequency и создаем index
                        if (resultFromLemma.next() && (resultFromLemma.getInt("site_id_id") == siteId)) {
                            int currentValue = resultFromLemma.getInt("frequency");
                            dbCommands.updateDbData("lemma", "frequency", String.valueOf(++currentValue),
                                    "site_id_id", String.valueOf(siteId));

                            indexEntity.setRankValue(entry.getValue());
                            LemmaEntity newLemma = new LemmaEntity();
                            newLemma.setId(resultFromLemma.getInt("id"));
                            indexEntity.setLemmaId(newLemma);
                            indexEntity.setPageId(indexingPage);
                            indexRepository.save(indexEntity);

                            indexEntity = new IndexEntity();
                        } else {
                            // Если лемма не существует в таблице lemma, то создаем lemma и index
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
                        }
                    }
                } else {
                    // Если адрес сайта отличается от переданной страницы, возвращаем ошибку
                    result.put("result", "false");
                    result.put("error", "Данная страница находится за пределами сайтов, указанных в конфигурационном файле");
                    return result;
                }
            } else {
                // Если переданная страница имеется в таблице page
                // получаем id страницы
                ResultSet resultSet = dbCommands.selectAllFromDb("page", "path", url);
                int pageId = 0;
                if (resultSet.next()) {
                    pageId = resultSet.getInt("id");
                }
                // Получаем все записи из index_table по полученному id
                resultSet = dbCommands.selectAllFromDb("index_table", "page_id_id", String.valueOf(pageId));
                // Проходим по всем записям в таблице index_table для причастных лемм
                while (resultSet.next()) {
                    String lemmaId = resultSet.getString("lemma_id_id");
                    ResultSet resultSetFromLemma = dbCommands.selectAllFromDb("lemma", "id", lemmaId);
                    if (resultSetFromLemma.next()) {
                        int currentValue = resultSetFromLemma.getInt("frequency");
                        // Если значение леммы больше 1, понижаем значение. Если 1 и менее -> удаляем запись из таблицы lemma
                        if (currentValue > 1) {
                            dbCommands.updateDbData("lemma", "frequency", String.valueOf(--currentValue),
                                    "id", lemmaId);
                        } else dbCommands.deleteFromDb("lemma", "id", lemmaId);
                    }
                }
                // Удаляем записи из таблицы index_table
                dbCommands.deleteFromDb("index_table","page_id_id", String.valueOf(pageId));

                // Удаляем записи из таблицы page
                dbCommands.deleteFromDb("page", "id", String.valueOf(pageId));
                indexPage(url);
            }
        } catch (SQLException | IOException ex) {
            throw new RuntimeException(ex);
        }
        result.put("result", "true");
        return result;
    }
}

