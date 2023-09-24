package searchengine.dto.indexing;

import lombok.Getter;
import searchengine.config.Site;
import searchengine.dto.db.DbCommands;
import searchengine.model.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ForkJoinPool;


@Getter
public class SiteIndexer extends Thread {
    private final Site site;
    private final SiteRepository siteRepository;
    private final PageRepository pageRepository;
    private final LemmaRepository lemmaRepository;
    private final IndexRepository indexRepository;
    private volatile Set<PageUrl> uniqueUrl;
    private final Map<String, Integer> lemmasCounter;
    private boolean isStopped;
    private ForkJoinPool pool;
    private final DbCommands dbCommands = new DbCommands();

    public SiteIndexer(Site site, SiteRepository siteRepository,
                       PageRepository pageRepository, LemmaRepository lemmaRepository,
                       IndexRepository indexRepository, boolean isStopped) {
        this.site = site;
        this.siteRepository = siteRepository;
        this.pageRepository = pageRepository;
        this.lemmaRepository = lemmaRepository;
        this.indexRepository = indexRepository;
        this.isStopped = isStopped;
        uniqueUrl = new HashSet<>();
        lemmasCounter = new HashMap<>();
    }

    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            this.siteEntityCreator();
        }
    }

    public void siteEntityCreator() {
        System.out.println("Start " + Thread.currentThread().getName() + "\n");
        try (Connection connection = dbCommands.getNewConnection()) {
            deleteDataFromDB(connection, site.getUrl());

            SiteEntity newSiteEntity = createSiteEntity();
            siteRepository.save(newSiteEntity);

            List<String> firstUrl = new ArrayList<>();
            firstUrl.add(site.getUrl());

            PageIndexer pageIndexer = new PageIndexer(newSiteEntity, firstUrl, this);

            pool = new ForkJoinPool();
            pool.invoke(pageIndexer);

            if (!newSiteEntity.getLastError().isEmpty()) {
                dbCommands.updateDbData("site", "status", Status.FAILED.name(), "url", site.getUrl());

                String sqlSelect = "SELECT * FROM site WHERE url ='" + site.getUrl() + "'";
                ResultSet rs = connection.createStatement().executeQuery(sqlSelect);
                if (rs.next()) {
                    String siteId = rs.getString("id");
                    String pageId;

                    String selectFromPage = "SELECT * FROM page WHERE site_id_id ='" + siteId + "'";
                    ResultSet pageRs = connection.createStatement().executeQuery(selectFromPage);
                    while (pageRs.next()) {
                        pageId = pageRs.getString("id");
                        dbCommands.deleteFromDb("index_table", "page_id_id", pageId);
                    }
                    dbCommands.deleteFromDb("page", "site_id_id", siteId);
                    dbCommands.deleteFromDb("lemma", "site_id_id", siteId);
                }
            } else {
                dbCommands.updateDbData("site", "status", Status.INDEXED.name(), "url", site.getUrl());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Stop " + Thread.currentThread().getName() + "\n");
        this.interrupt();
    }

    private SiteEntity createSiteEntity() {
        SiteEntity newSiteEntity = new SiteEntity();
        newSiteEntity.setLastError("");
        newSiteEntity.setName(site.getName());
        newSiteEntity.setStatus(Status.INDEXING);
        newSiteEntity.setStatusTime(LocalDateTime.now());
        String siteUrl = site.getUrl();
        StringBuilder siteUrlWithHttps = new StringBuilder(site.getUrl());
        if (siteUrl.charAt(4) != 's') {
            siteUrlWithHttps.insert(4, 's');
        }
        newSiteEntity.setUrl(siteUrlWithHttps.toString());
        return newSiteEntity;
    }


    private void deleteDataFromDB(Connection connection, String url) {
        String sqlSelect = "SELECT * FROM site WHERE url='" + url + "'";
        try (ResultSet rs = connection.createStatement().executeQuery(sqlSelect)) {
            if (rs.next()) {
                dbCommands.deleteFromDb("site", "url", url);
                String siteId = rs.getString("id");
                String pageId;

                String selectFromPage = "SELECT * FROM page WHERE site_id_id='" + siteId + "'";
                try (ResultSet pageRs = connection.createStatement().executeQuery(selectFromPage)) {
                    while (pageRs.next()) {
                        pageId = pageRs.getString("id");
                        dbCommands.deleteFromDb("index_table", "page_id_id", pageId);
                    }
                }
                dbCommands.deleteFromDb("page", "site_id_id", siteId);
                dbCommands.deleteFromDb("lemma", "site_id_id", siteId);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
