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
import java.sql.Connection;
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

        try (Connection connection = dbCommands.getNewConnection()) {
            lemmatizator = Lemmatizator.getInstance();

            // Получаем леммы из запроса
            Map<String, Integer> lemmasFromQuery = lemmatizator.collectLemmas(query);

            // Получаем листы лемм и фильтруем от встречающихся на слишком большом количестве страниц
            Map<Integer, List<LemmaEntity>> lemmasListFromSites;

            // Если задан конкретный сайт для поиска совпадений, ищем его в БД, для дальнейшей работы только с ним
            if (site != null) {
                String sqlSelect = "SELECT * FROM site WHERE url ='" + site + "'";
                ResultSet rs = connection.createStatement().executeQuery(sqlSelect);
                if (rs.next()) {
                    searchingSiteId = rs.getInt("id");
                }
                lemmasListFromSites = getAndFilterLemmasFromOneSite(connection, lemmasFromQuery, searchingSiteId);
            } else {
                lemmasListFromSites = getAndFilterLemmasFromSites(connection, lemmasFromQuery);
            }

            // Сортировка лемм в порядке возрастания frequency
            for (var entry : lemmasListFromSites.entrySet()) {
                entry.setValue(entry.getValue().stream().sorted(Comparator.comparing(LemmaEntity::getFrequency))
                        .toList());
            }

            // Map для перечня IndexEntity, на которых встречались данные леммы. Ключ = номер страницы в БД
            Map<Integer, IndexEntity> totalIndexEntities = new HashMap<>();

            List<SearchingData> searchingDataList;
            List<SearchingData> searchingDataListBeforeSort = new ArrayList<>();
            int totalPagesCount = 0;

            // Обходим LemmaEntity, исключаем страницы на которых нет предыдущих лемм, сохраняем в totalIndexEntities
            for (var entry : lemmasListFromSites.entrySet()) {
                List<LemmaEntity> sortedLemmas = entry.getValue();

                sortedLemmas.forEach(lemmaEntity -> {
                    Map<Integer, IndexEntity> indexEntities = new HashMap<>();

                    String selectFromIndex = "SELECT * FROM index_table WHERE lemma_id_id ='" + lemmaEntity.getId() + "'";

                    try {
                        ResultSet indexRs = connection.createStatement().executeQuery(selectFromIndex);
                        while (indexRs.next()) {
                            IndexEntity newIndexEntity = new IndexEntity();
                            newIndexEntity.setRankValue(indexRs.getFloat("rank_value"));
                            newIndexEntity.setLemmaId(lemmaEntity);
                            PageEntity currentPage = new PageEntity();
                            currentPage.setId(indexRs.getInt("page_id_id"));
                            newIndexEntity.setPageId(currentPage);
                            indexEntities.put(currentPage.getId(), newIndexEntity);
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
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
                Map<Integer, Float> relevanceValues = new HashMap<>();

                for (int i : totalIndexEntities.keySet()) {
                    SearchingData data = new SearchingData();
                    int siteId = 0;
                    String path = null;

                    // Получение сведений о сайте
                    String selectFromPage = "SELECT * FROM page WHERE id ='" + totalIndexEntities.get(i).getPageId().getId() + "'";
                    ResultSet pageRs = connection.createStatement().executeQuery(selectFromPage);
                    if (pageRs.next()) {
                        siteId = pageRs.getInt("site_id_id");
                        path = pageRs.getString("path");
                    }

                    String siteName = null;
                    String url = null;

                    String selectFromSite = "SELECT * FROM site WHERE id ='" + siteId + "'";
                    ResultSet siteRs = connection.createStatement().executeQuery(selectFromSite);
                    if (siteRs.next()) {
                        siteName = siteRs.getString("name");
                        url = siteRs.getString("url");
                    }

                    if (url != null && path != null) {
                        data.setSite(url);
                        data.setSiteName(siteName);
                        String[] urlArray = path.split(url);

                        if (urlArray.length > 0) {
                            data.setUri(urlArray[1]);
                        } else {
                            data.setUri("");
                        }
                    }

                    // Получаем релевантность
                    if (relevanceValues.isEmpty()) {
                        relevanceValues = calculateRelevance(connection, sortedLemmas, totalIndexEntities);
                    }
                    data.setRelevance(relevanceValues.get(totalIndexEntities.get(i).getPageId().getId()));

                    // Получаем сниппеты
                    List<String> snippetList = new ArrayList<>();

                    String pageData = null;

                    selectFromPage = "SELECT * FROM page WHERE id ='" + i + "'";

                    pageRs = connection.createStatement().executeQuery(selectFromPage);
                    if (pageRs.next()) {
                        String content = pageRs.getString("content");
                        int startTitle = content.indexOf("<title>") + 7;
                        int endTitle = content.indexOf("</title>");
                        String title = content.substring(startTitle, endTitle);
                        data.setTitle(title);
                        pageData = Jsoup.parse(content).text();
                    }

                    String[] queryArray = query.trim().split(" ");

                    // Формируем строку для поиска по тексту страницы, с послудющим уменьшением
                    // используемых лемм, от общего количества к одной лемме
                    createSnippet(queryArray, pageData, snippetList);

                    if (snippetList.isEmpty()) {
                        data.setSnippet("Нет совпадений на этой странице");
                    } else {
                        data.setSnippet(snippetList.get(0));
                    }
                    searchingDataListBeforeSort.add(data);
                }
            }

            // Сортировка SearchingData объектов по релевантности от большей к меньшей
            searchingDataList = searchingDataListBeforeSort.stream().sorted(Comparator.
                    comparing(SearchingData::getRelevance).
                    reversed()).toList();

            response.setResult(true);
            response.setCount(totalPagesCount);
            response.setData(searchingDataList);

            System.out.println("Поиск страниц завершен за " + (System.currentTimeMillis() - start) + " миллисекунд");

            return response;
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<Integer, List<LemmaEntity>> getAndFilterLemmasFromSites(Connection connection,
                                                                        Map<String, Integer> lemmasFromQuery) throws SQLException {
        Map<Integer, List<LemmaEntity>> result = new HashMap<>();
        for (var entry : lemmasFromQuery.entrySet()) {
            String selectLemmasFrequency = "SELECT * FROM lemma WHERE `lemma` = '" + entry.getKey() + "'";
            ResultSet lemmaRs = connection.createStatement().executeQuery(selectLemmasFrequency);
            while (true) {
                try {
                    if (!lemmaRs.next()) break;
                    int siteId = lemmaRs.getInt("site_id_id");
                    LemmaEntity newLemma = new LemmaEntity();
                    newLemma.setId(lemmaRs.getInt("id"));
                    newLemma.setFrequency(lemmaRs.getInt("frequency"));
                    newLemma.setLemma(entry.getKey());
                    SiteEntity newSite = new SiteEntity();
                    newSite.setId(siteId);
                    newLemma.setSiteId(newSite);
                    if (!result.containsKey(siteId)) {
                        result.put(siteId, new ArrayList<>());
                        result.get(siteId).add(newLemma);
                    } else {
                        result.get(siteId).add(newLemma);
                    }

                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        removeFrequentLemmas(connection, result);

        return result;
    }

    private Map<Integer, List<LemmaEntity>> getAndFilterLemmasFromOneSite(Connection connection,
                                                                        Map<String, Integer> lemmasFromQuery,
                                                                          int siteId) throws SQLException {
        Map<Integer, List<LemmaEntity>> result = new HashMap<>();
        for (var entry : lemmasFromQuery.entrySet()) {
            String selectLemmasFrequency = "SELECT * FROM lemma WHERE `lemma` = '" +
                    entry.getKey() + "' AND site_id_id = '" + siteId + "'";
            ResultSet lemmaRs = connection.createStatement().executeQuery(selectLemmasFrequency);
            while (true) {
                try {
                    if (!lemmaRs.next()) break;
                    LemmaEntity newLemma = new LemmaEntity();
                    newLemma.setId(lemmaRs.getInt("id"));
                    newLemma.setFrequency(lemmaRs.getInt("frequency"));
                    newLemma.setLemma(entry.getKey());
                    SiteEntity newSite = new SiteEntity();
                    newSite.setId(siteId);
                    newLemma.setSiteId(newSite);
                    if (!result.containsKey(siteId)) {
                        result.put(siteId, new ArrayList<>());
                        result.get(siteId).add(newLemma);
                    } else {
                        result.get(siteId).add(newLemma);
                    }

                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        removeFrequentLemmas(connection, result);

        return result;
    }

    private void removeFrequentLemmas(Connection connection, Map<Integer, List<LemmaEntity>> lemmasListFromSites) {
        for (var entry : lemmasListFromSites.keySet()) {
            List<LemmaEntity> oldLemmaEntities = lemmasListFromSites.get(entry);
            List<LemmaEntity> newLemmaEntities = new ArrayList<>();
            for (LemmaEntity lemma : oldLemmaEntities) {
                int lemmasPercent = getPercent(connection, lemma);
                if (lemmasPercent < 80) {
                    newLemmaEntities.add(lemma);
                }
            }
            lemmasListFromSites.put(entry, newLemmaEntities);
        }
    }

    private int getPercent(Connection connection, LemmaEntity lemmas) {
        float totalPageCount = 0;
        try {
            ResultSet rs = connection.createStatement().
                    executeQuery("SELECT COUNT(*) FROM page WHERE site_id_id = '" + lemmas.getSiteId().getId() + "'");
            if (rs.next()) {
                totalPageCount = rs.getInt("COUNT(*)");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return Math.round(((float) lemmas.getFrequency() / totalPageCount) * 100);
    }

    private Map<Integer, Float> calculateRelevance
            (Connection connection, List<LemmaEntity> sortedLemmas, Map<Integer, IndexEntity> totalIndexEntities) {
        Map<Integer, Float> result = new HashMap<>();
        float maxRelevant = 0;

        for (var i : totalIndexEntities.keySet()) {
            float absRelevant = 0;
            for (LemmaEntity lemma : sortedLemmas) {
                String selectFromIndex = "SELECT * FROM index_table WHERE page_id_id ='" + totalIndexEntities.get(i).getPageId().getId() + "' and lemma_id_id ='" + lemma.getId() + "'";
                try {
                    ResultSet indexRs = connection.createStatement().executeQuery(selectFromIndex);
                    if (indexRs.next()) {
                        absRelevant += indexRs.getFloat("rank_value");
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
        Pattern patternIgnoreCase = Pattern.compile(getPatternForContainsIgnoreCase(findText));
        Matcher matcher = patternIgnoreCase.matcher(text);

        while (matcher.find()) {
            matches.add(matcher.group());
        }
        return matches;
    }

    private void createSnippet(String[] queryArray, String pageData, List<String> snippetList) {
        for (int j = queryArray.length - 1; j >= 0; j--) {
            StringBuilder lemmasCollector = new StringBuilder();
            for (int k = 0; k < j + 1; k++) {
                lemmasCollector.append(queryArray[k]).append(" ");
            }

            Set<String> sentenceMatches = new HashSet<>();

            String patternForLemmas = getPatternForLemmas(lemmasCollector.toString());
            Pattern pattern = Pattern.compile(patternForLemmas);
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
                            sentence = trimSnippetSentence(sentence, currentLemma);
                            sentence = sentence.replace(currentLemma, "<b>" + currentLemma + "</b>");
                            wordForEquals.append(currentLemma);
                        }
                    }
                }
                snippetList.add(sentence);
            }
        }
    }

    private String trimSnippetSentence(String sentence, String lemma) {

        StringBuilder result = new StringBuilder();

        List<String> sentencePart = List.of(sentence.split(lemma));
        for (int i = 0; i < sentencePart.size(); i++) {
            if ((i % 2) == 0 && sentencePart.get(i).length() > 120) {
                String currentPart = sentencePart.get(i);
                if (i > 0) {
                    result.append(currentPart, 0, 120);
                } else {
                    result.append("...");
                    result.append(currentPart, currentPart.length() - 120, currentPart.length());
                }
                if (i != sentencePart.size() - 1) {
                    result.append(lemma);
                }
            }

            if ((i % 2) == 0 && sentencePart.get(i).length() < 120) {
                String currentPart = sentencePart.get(i);
                result.append(currentPart, 0, currentPart.length());
                if (i != sentencePart.size() - 1) {
                    result.append(lemma);
                }
            }

            if ((i % 2) != 0 && sentencePart.get(i).length() > 120) {
                String currentPart = sentencePart.get(i);
                result.append(currentPart, 0, 120);
                result.append("...");
                if (i != sentencePart.size() - 1) {
                    result.append(lemma);
                }
            }

            if ((i % 2) != 0 && sentencePart.get(i).length() < 120) {
                String currentPart = sentencePart.get(i);
                result.append(currentPart, 0, currentPart.length());
                if (i != sentencePart.size() - 1) {
                    result.append(lemma);
                }
            }
        }

        if (result.length() > 300) {
            result = new StringBuilder(result.substring(0, 249));
        }

        return result.toString();
    }
}

