package searchengine.services;

import org.jsoup.Jsoup;
import org.springframework.stereotype.Service;
import searchengine.dto.db.DbCommands;
import searchengine.dto.indexing.Lemmatizator;
import searchengine.dto.search.SearchingData;
import searchengine.dto.search.SearchingResponse;
import searchengine.model.IndexEntity;
import searchengine.model.LemmaEntity;
import searchengine.model.PageEntity;
import searchengine.model.SiteEntity;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class SearchingServiceImpl implements SearchingService {

    private Lemmatizator lemmatizator;

    private final DbCommands dbCommands = new DbCommands();

    @Override
    public SearchingResponse search(String query, String site) {
        long start = System.currentTimeMillis();

        Integer searchingSiteId = null;

        SearchingResponse response = new SearchingResponse();

        try {
            lemmatizator = Lemmatizator.getInstance();
            // Если задан конкретный сайт для поиска совпадений, ищем его в БД, для дальнейшей работы только с ним
            if (site != null) {
                ResultSet resultSet = dbCommands.selectFromDbWithParameters("site", "url", site);
                if (resultSet.next()) {
                    searchingSiteId = resultSet.getInt("id");
                }
            }
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }

        Map<String, Integer> lemmasFromQuery = lemmatizator.collectLemmas(query);

        // Map для соотношения лемм к каждому отдельному сайту
        Map<Integer, List<LemmaEntity>> differentSitesLemmasList = new HashMap<>();

        // Обходим леммы, сохраняем получившиеся LemmaEntity. Исключаем часто встречающиеся.
        for (var entry : lemmasFromQuery.entrySet()) {
            ResultSet resultSetFromLemma = dbCommands.selectFromDbLessThan("lemma", "lemma",
                    "frequency", entry.getKey(), 100);
            while (true) {
                try {
                    if (!resultSetFromLemma.next()) break;

                    LemmaEntity newLemma = new LemmaEntity();
                    newLemma.setId(resultSetFromLemma.getInt("id"));
                    newLemma.setFrequency(resultSetFromLemma.getInt("frequency"));
                    newLemma.setLemma(resultSetFromLemma.getString("lemma"));
                    SiteEntity newSite = new SiteEntity();
                    int id = resultSetFromLemma.getInt("site_id_id");
                    if (searchingSiteId == null || searchingSiteId == id) {
                        newSite.setId(id);
                        newLemma.setSiteId(newSite);
                        if (differentSitesLemmasList.containsKey(id)) {
                            differentSitesLemmasList.get(id).add(newLemma);
                        } else {
                            List<LemmaEntity> lemmasList = new ArrayList<>();
                            lemmasList.add(newLemma);
                            differentSitesLemmasList.put(id, lemmasList);
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        // Сортировка лемм в порядке возрастания frequency
        for (var entry : differentSitesLemmasList.entrySet()) {
            entry.setValue(entry.getValue().stream().sorted(Comparator.comparing(LemmaEntity::getFrequency))
                    .toList());
        }

        // Map для переченя IndexEntity, на которых встречались данные леммы. Ключ = номер страницы в БД
        Map<Integer, IndexEntity> totalIndexEntities = new HashMap<>();

        List<SearchingData> searchingDataList;
        List<SearchingData> searchingDataListBeforeSort = new ArrayList<>();
        int totalPagesCount = 0;

        // Обходим LemmaEntity, исключаем страницы на которых нет предыдущих лемм, сохраняем в totalIndexEntities
        for (var entry : differentSitesLemmasList.entrySet()) {
            List<LemmaEntity> sortedLemmas = entry.getValue();

            sortedLemmas.forEach(lemmaEntity -> {
                Map<Integer, IndexEntity> indexEntities = new HashMap<>();

                ResultSet resultSetFromIndexTable = dbCommands.selectFromDbWithParameters("index_table",
                        "lemma_id_id", String.valueOf(lemmaEntity.getId()));

                while (true) {
                    try {
                        if (!resultSetFromIndexTable.next()) break;
                        IndexEntity newIndexEntity = new IndexEntity();
                        newIndexEntity.setRankValue(resultSetFromIndexTable.getFloat("rank_value"));
                        newIndexEntity.setLemmaId(lemmaEntity);
                        PageEntity currentPage = new PageEntity();
                        currentPage.setId(resultSetFromIndexTable.getInt("page_id_id"));
                        newIndexEntity.setPageId(currentPage);
                        indexEntities.put(currentPage.getId(), newIndexEntity);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
                // Проверяем страницы текущей леммы и предыдущей, если совпали, то оставляем, если нет удаляем
                if (totalIndexEntities.isEmpty()) {
                    totalIndexEntities.putAll(indexEntities);
                } else {
                    Map<Integer, IndexEntity> tempMap = new HashMap<>();
                    for (int i : indexEntities.keySet()) {
                        if (totalIndexEntities.containsKey(i)) {
                            tempMap.put(i, totalIndexEntities.get(i));
                        }
                    }
                    totalIndexEntities.clear();
                    totalIndexEntities.putAll(tempMap);
                }
            });

            totalPagesCount += totalIndexEntities.size();

            for (int i : totalIndexEntities.keySet()) {
                SearchingData data = new SearchingData();

                // Получение сведений о сайте
                ResultSet resultSetFromPageTable = dbCommands.selectFromDbWithParameters("page", "id",
                        String.valueOf(totalIndexEntities.get(i).getPageId().getId()));
                int siteId = 0;
                String path = null;
                try {
                    if (resultSetFromPageTable.next()) {
                        siteId = resultSetFromPageTable.getInt("site_id_id");
                        path = resultSetFromPageTable.getString("path");
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

                ResultSet resultSetFromSiteTable = dbCommands.selectFromDbWithParameters("site", "id",
                        String.valueOf(siteId));
                String siteName = null;
                String url = null;
                try {
                    if (resultSetFromSiteTable.next()) {
                        siteName = resultSetFromSiteTable.getString("name");
                        url = resultSetFromSiteTable.getString("url");
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

                if (url != null && path != null) {
                    data.setSite(url);
                    data.setSiteName(siteName);
                    String[] urlArray = path.split(url);

                    if (urlArray.length == 1) {
                        data.setUri(urlArray[0]);
                    } else {
                        data.setUri(urlArray[1]);
                    }
                }

                // Получаем релевантность
                Map<Integer, Float> relRelevance = calculateRelevance(sortedLemmas, totalIndexEntities);
                data.setRelevance(relRelevance.get(totalIndexEntities.get(i).getPageId().getId()));

                // Получаем сниппеты
                List<String> snippetList = new ArrayList<>();

                try {
                    String pageData = null;
                    ResultSet resultSetForPageDate = dbCommands.selectFromDbWithParameters("page",
                            "id", String.valueOf(i));

                    if (resultSetForPageDate.next()) {
                        String content = resultSetForPageDate.getString("content");
                        int startTitle = content.indexOf("<title>") + 7;
                        int endTitle = content.indexOf("</title>");
                        String title = content.substring(startTitle, endTitle);
                        data.setTitle(title);
                        pageData = Jsoup.parse(content).text();
                    }

                    String[] queryArray = query.trim().split(" ");

                    // Формируем строку для поиска по тексту страницы, с послудющим уменьшением
                    // используемых лемм, от общего количества к одной лемме
                    for (int j = queryArray.length - 1; j >= 0; j--) {
                        StringBuilder lemmasCollector = new StringBuilder();
                        for (int k = 0; k < j + 1; k++) {
                            lemmasCollector.append(queryArray[k]).append(" ");
                        }

                        Set<String> sentenceMatches = new HashSet<>();
                        // Поиск предложений содержащих искомые леммы
                        String stringForPattern = getPatternForLemmas(lemmasCollector.toString());
                        Pattern pattern = Pattern.compile(stringForPattern);
                        Matcher matcher = pattern.matcher(pageData);
                        while (matcher.find()) {
                            String sentence = matcher.group();
                            for (String lemma : queryArray) {
                                List<String> lemmaVariables = getMatches(lemma, sentence);
                                if (!lemmaVariables.isEmpty()) {
                                    sentenceMatches.add(sentence);
                                }
                            }
                        }

                        // Добавление тега <b> вокруг искомых лемм
                        for (String sentence : sentenceMatches) {
                            for (String lemma : queryArray) {
                                List<String> lemmaVariables = getMatches(lemma, sentence);
                                if (!lemmaVariables.isEmpty()) {
                                    StringBuilder wordForEquals = new StringBuilder();
                                    for (String currentLemma : lemmaVariables) {
                                        if (wordForEquals.toString().contains(currentLemma)) {
                                            continue;
                                        }
                                        sentence = sentence.replace(currentLemma, "<b>" + currentLemma + "</b>");
                                        wordForEquals.append(currentLemma);
                                    }
                                }
                            }
                            snippetList.add(sentence);
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

                if (snippetList.isEmpty()) {
                    data.setSnippet("Нет совпадений на этой странице");
                } else {
                    data.setSnippet(snippetList.get(0));
                }
                searchingDataListBeforeSort.add(data);
            }
            System.out.println();
        }

        // Сортировка SearchingData объектов по релевантности от большей к меньшей
        searchingDataList = searchingDataListBeforeSort.stream().sorted(Comparator.
                comparing(SearchingData::getRelevance).
                reversed()).toList();

        if (searchingDataList.size() > 5) {
            searchingDataList = searchingDataList.subList(0, 5);
        }

        response.setResult(true);
        response.setCount(totalPagesCount);
        response.setData(searchingDataList);

        System.out.println("Поиск страниц завершен за " + (System.currentTimeMillis() - start) + " миллисекунд");

        return response;
    }

    private Map<Integer, Float> calculateRelevance
            (List<LemmaEntity> sortedLemmas, Map<Integer, IndexEntity> totalIndexEntities) {
        Map<Integer, Float> result = new HashMap<>();
        float maxRelevant = 0;
        for (var i : totalIndexEntities.keySet()) {
            float absRelevant = 0;
            for (LemmaEntity lemma : sortedLemmas) {
                ResultSet resultSetFromIndexTable = dbCommands.selectFromDbWithTwoParameters("index_table",
                        "page_id_id", "lemma_id_id",
                        String.valueOf(totalIndexEntities.get(i).getPageId().getId()),
                        String.valueOf(lemma.getId()));
                try {
                    if (resultSetFromIndexTable.next()) {
                        absRelevant += resultSetFromIndexTable.getFloat("rank_value");
                        result.put(totalIndexEntities.get(i).getPageId().getId(), absRelevant);
                        if (maxRelevant < absRelevant) {
                            maxRelevant = absRelevant;
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        for (Map.Entry<Integer, Float> entry : result.entrySet()) {
            float abs = entry.getValue();
            entry.setValue(abs / maxRelevant);
        }
        return result;
    }

    private String getPatternForLemmas(String query) {
        String startPattern = "(?=[^.]*(?:";
        StringBuilder pattern = new StringBuilder();
        String[] queryArray = query.trim().split(" ");

        if (queryArray.length != 0) {
            String upper = queryArray[0].substring(0, 1).toUpperCase(Locale.ROOT);
            String lower = queryArray[0].substring(0, 1).toLowerCase(Locale.ROOT);
            String subStr = queryArray[0].substring(1);
            pattern.append(startPattern).append(upper).append("|").append(lower).append(")")
                    .append(subStr).append(")((?:^|[A-ZА-Я0-9Ё])[^.]+[!?]|(?:^|[A-ZА-Я0-9Ё])[^.]+\\.)");

            if (queryArray.length > 1) {
                int indexForInsert = pattern.indexOf("((?:");
                String secondHalfForRegEx = pattern.substring(indexForInsert);

                pattern = new StringBuilder("(");
                for (int i = 0; i < queryArray.length; i++) {
                    upper = queryArray[i].substring(0, 1).toUpperCase(Locale.ROOT);
                    lower = queryArray[i].substring(0, 1).toLowerCase(Locale.ROOT);
                    subStr = queryArray[i].substring(1);

                    pattern.append(startPattern).append(upper).append("|").append(lower).append(")")
                            .append(subStr).append(")");
                    if (i == queryArray.length - 1) {
                        pattern.append(")");
                    }
                }
                pattern.append(secondHalfForRegEx);
            }
        }
        return pattern.toString();
    }

    private String getPatternForContainsIgnoreCase(String word) {
        StringBuilder pattern = new StringBuilder();

        pattern.append("(\\b");
        for (int i = 0; i < word.length(); i++) {
            pattern.append("[");
            String ch = String.valueOf(word.charAt(i));
            pattern.append(ch.toUpperCase());
            pattern.append(ch.toLowerCase());
            pattern.append("]");
        }
        pattern.append("\\b)");
        return pattern.toString();
    }

    private List<String> getMatches(String findText, String text) {
        List<String> matches = new ArrayList<>();
        Pattern pattern = Pattern.compile(getPatternForContainsIgnoreCase(findText));
        Matcher matcher = pattern.matcher(text);

        while (matcher.find()) {
            matches.add(matcher.group());
        }
        return matches;
    }
}

