package searchengine.services;

import org.springframework.beans.factory.annotation.Autowired;
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
import java.util.stream.Collectors;

@Service
public class SearchingServiceImpl implements SearchingService {

    private Lemmatizator lemmatizator;

    private final DbCommands dbCommands = new DbCommands();

    @Override
    public SearchingResponse search(String query) {
        SearchingResponse response = new SearchingResponse();
        try {
            lemmatizator = Lemmatizator.getInstance();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Map<String, Integer> lemmasFromQuery = lemmatizator.collectLemmas(query);

        List<LemmaEntity> lemmasList = new ArrayList<>();

        for (var entry : lemmasFromQuery.entrySet()) {
            ResultSet resultSetFromLemma = dbCommands.selectFromDbLessThan("lemma", "lemma",
                    "frequency", entry.getKey(), 60);
            while (true) {
                try {
                    if (!resultSetFromLemma.next()) break;

                    LemmaEntity newLemma = new LemmaEntity();
                    newLemma.setId(resultSetFromLemma.getInt("id"));
                    newLemma.setFrequency(resultSetFromLemma.getInt("frequency"));
                    newLemma.setLemma(resultSetFromLemma.getString("lemma"));
                    SiteEntity newSite = new SiteEntity();
                    newSite.setId(resultSetFromLemma.getInt("site_id_id"));
                    newLemma.setSiteId(newSite);
                    lemmasList.add(newLemma);

                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        List<LemmaEntity> sortedLemmas = lemmasList.stream().sorted(Comparator.comparing(LemmaEntity::getFrequency))
                .toList();

        Map<Integer, IndexEntity> totalIndexEntities = new HashMap<>();

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
            if (totalIndexEntities.isEmpty()) {
                totalIndexEntities.putAll(indexEntities);
            } else {
                Map<Integer, IndexEntity> tempMap = new HashMap<>();
                for(int i : indexEntities.keySet()) {
                    if (totalIndexEntities.containsKey(i)) {
                        tempMap.put(i,totalIndexEntities.get(i));
                    }
                }
                totalIndexEntities.clear();
                totalIndexEntities.putAll(tempMap);
            }
        });


        // Расчет релевантности
        for(int i : totalIndexEntities.keySet()) {
            System.out.println(totalIndexEntities.get(i).getPageId().getId() + " : rank - " + totalIndexEntities.get(i).getRankValue());
        }


        SearchingData date = new SearchingData();


        return response;
    }
}

