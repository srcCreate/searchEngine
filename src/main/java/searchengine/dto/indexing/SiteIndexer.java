package searchengine.dto.indexing;

import searchengine.config.Site;
import searchengine.dto.db.DbCommands;
import searchengine.model.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.RecursiveAction;


public class SiteIndexer extends RecursiveAction {
    private final List<Site> sites;
    private final List<SiteIndexer> siteIndexerList = new ArrayList<>();
    private final SiteRepository siteRepository;
    private final PageRepository pageRepository;
    private final LemmaRepository lemmaRepository;
    private final IndexRepository indexRepository;
    private final boolean isStoped;
    private DbCommands dbCommands = new DbCommands();

    public SiteIndexer(List<Site> sites, SiteRepository siteRepository,
                       PageRepository pageRepository, LemmaRepository lemmaRepository,
                       IndexRepository indexRepository, boolean isStoped) {
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

        //Объединяем каждый ответвленный SiteIndexеr
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
        deleteDataFromDB(site.getUrl());

        SiteEntity newSiteEntity = new SiteEntity();
        newSiteEntity.setLastError("");
        newSiteEntity.setName(site.getName());
        newSiteEntity.setStatus(Status.INDEXING);
        newSiteEntity.setStatusTime(LocalDateTime.now());
        newSiteEntity.setUrl(site.getUrl());
        siteRepository.save(newSiteEntity);

        Map<String, Integer> lemmasCounter = new HashMap<>();

        PageIndexer pageIndexer = new PageIndexer(newSiteEntity, pageRepository, lemmasCounter, lemmaRepository, indexRepository);
        pageIndexer.parsePages();

        if (!newSiteEntity.getLastError().isEmpty()) {
            dbCommands.updateDbData("site", "status", Status.FAILED.name(), "url", site.getUrl());
            ResultSet resultSet = dbCommands.selectFromDbWithParameters("site", "url", site.getUrl());
            try {
                if (resultSet.next()) {
                    String siteId = resultSet.getString("id");
                    String pageId;
                    ResultSet resultSetFromPage = dbCommands.selectFromDbWithParameters("page", "site_id_id", siteId);
                    while (resultSetFromPage.next()) {
                        pageId = resultSetFromPage.getString("id");
                        dbCommands.deleteFromDb("index_table","page_id_id",pageId);
                    }
                    dbCommands.deleteFromDb("page", "site_id_id", siteId);
                    dbCommands.deleteFromDb("lemma","site_id_id", siteId);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else {
            dbCommands.updateDbData("site", "status", Status.INDEXED.name(), "url", site.getUrl());
        }

        System.out.println("Stop " + Thread.currentThread().getName() + "\n");
    }

    private void deleteDataFromDB(String url) {
        ResultSet resultSet = dbCommands.selectFromDbWithParameters("site", "url", url);
        try {
            if (resultSet.next()) {
                dbCommands.deleteFromDb("site", "url", url);
                String siteId = resultSet.getString("id");
                String pageId;
                ResultSet resultSetFromPage = dbCommands.selectFromDbWithParameters("page", "site_id_id", siteId);
                while (resultSetFromPage.next()) {
                    pageId = resultSetFromPage.getString("id");
                    dbCommands.deleteFromDb("index_table","page_id_id",pageId);
                }
                dbCommands.deleteFromDb("page", "site_id_id", siteId);
                dbCommands.deleteFromDb("lemma","site_id_id", siteId);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
