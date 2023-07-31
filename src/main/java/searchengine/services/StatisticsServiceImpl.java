package searchengine.services;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import searchengine.config.SitesList;
import searchengine.dto.statistics.DetailedStatisticsItem;
import searchengine.dto.statistics.StatisticsData;
import searchengine.dto.statistics.StatisticsResponse;
import searchengine.dto.statistics.TotalStatistics;
import searchengine.model.PageEntity;
import searchengine.model.SiteEntity;
import searchengine.model.Status;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.Instant.ofEpochMilli;

@Service
@RequiredArgsConstructor
public class StatisticsServiceImpl implements StatisticsService {

    private final SitesList sites;

    /*@Override
    public StatisticsResponse getStatistics() {
        String[] statuses = { "INDEXED", "FAILED", "INDEXING" };
        String[] errors = {
                "Ошибка индексации: главная страница сайта не доступна",
                "Ошибка индексации: сайт не доступен",
                ""
        };

        TotalStatistics total = new TotalStatistics();
        total.setSites(sites.getSites().size());
        total.setIndexing(true);

        List<DetailedStatisticsItem> detailed = new ArrayList<>();
        List<Site> sitesList = sites.getSites();
        for(int i = 0; i < sitesList.size(); i++) {
            Site site = sitesList.get(i);
            DetailedStatisticsItem item = new DetailedStatisticsItem();
            item.setName(site.getName());
            item.setUrl(site.getUrl());
            int pages = random.nextInt(1_000);
            int lemmas = pages * random.nextInt(1_000);
            item.setPages(pages);
            item.setLemmas(lemmas);
            item.setStatus(statuses[i % 3]);
            item.setError(errors[i % 3]);
            item.setStatusTime(System.currentTimeMillis() -
                    (random.nextInt(10_000)));
            total.setPages(total.getPages() + pages);
            total.setLemmas(total.getLemmas() + lemmas);
            detailed.add(item);
        }

        StatisticsResponse response = new StatisticsResponse();
        StatisticsData data = new StatisticsData();
        data.setTotal(total);
        data.setDetailed(detailed);
        response.setStatistics(data);
        response.setResult(true);
        return response;
    }*/

    @Override
    public StatisticsResponse getStatistics() {

        TotalStatistics total = new TotalStatistics();
        total.setSites(sites.getSites().size());
        total.setIndexing(true);

        List<DetailedStatisticsItem> detailed = new ArrayList<>();
        ResultSet allSitesResultSet = selectAllFromDb("site");
        List<SiteEntity> allSites = new ArrayList<>();

        while (true) {
            try {
                if (!allSitesResultSet.next()) break;
                SiteEntity site = new SiteEntity();
                site.setId(allSitesResultSet.getInt("id"));
                site.setLastError(allSitesResultSet.getString("last_error"));
                site.setName(allSitesResultSet.getString("name"));
                site.setStatus(Status.valueOf(allSitesResultSet.getString("status")));
                long timestamp = allSitesResultSet.getTimestamp("status_time").getTime();
                site.setStatusTime(LocalDateTime.ofInstant(ofEpochMilli(timestamp), TimeZone.getDefault().toZoneId()));
                site.setUrl(allSitesResultSet.getString("url"));
                allSites.add(site);

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        for (int i = 0; i < allSites.size(); i++) {
            SiteEntity site = allSites.get(i);
            DetailedStatisticsItem item = new DetailedStatisticsItem();
            item.setName(site.getName());
            item.setUrl(site.getUrl());
            ResultSet resultSetFromPageTable = selectFromDb(String.valueOf(site.getId()), "page", "site_id_id");
            List<PageEntity> pages = new ArrayList<>();
            while (true) {
                try {
                    if (!resultSetFromPageTable.next()) break; {
                        PageEntity page = new PageEntity();
                        page.setId(resultSetFromPageTable.getInt("id"));
                        page.setCode(resultSetFromPageTable.getInt("code"));
                        page.setContent(resultSetFromPageTable.getString("content"));
                        page.setPath(resultSetFromPageTable.getString("path"));
                        page.setSiteId(site);
                        pages.add(page);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            AtomicInteger lemmasCounter = new AtomicInteger();
            pages.forEach(page -> {
                ResultSet countLemmas = selectCountFromDb(String.valueOf(page.getId()), "index_table", "page_id_id");
                try {
                    if (countLemmas.next()) {
                        int currentPageCount = countLemmas.getInt("COUNT(*)");
                        lemmasCounter.addAndGet(currentPageCount);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            int lemmas = lemmasCounter.get();


            item.setPages(pages.size());
            item.setLemmas(lemmas);
            item.setStatus(String.valueOf(site.getStatus()));
            if (!site.getLastError().isEmpty()) {
                item.setError(site.getLastError());
            } else {
                item.setError("");
            }
            item.setStatusTime(site.getStatusTime().atZone(ZoneId.systemDefault()).toEpochSecond());
            total.setPages(total.getPages() + pages.size());
            total.setLemmas(total.getLemmas() + lemmas);
            detailed.add(item);
        }

        StatisticsResponse response = new StatisticsResponse();
        StatisticsData data = new StatisticsData();
        data.setTotal(total);
        data.setDetailed(detailed);
        response.setStatistics(data);
        response.setResult(true);
        return response;
    }

    private ResultSet selectFromDb(String data, String table, String column) {
        String sqlSelect = "SELECT * FROM " + table + " WHERE " + column + "='" + data + "'";
        Statement stmt;
        try {
            java.sql.Connection connect = DriverManager.
                    getConnection("jdbc:mysql://localhost/search_engine", "root", "pass");
            stmt = connect.createStatement();
            return stmt.executeQuery(sqlSelect);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private ResultSet selectCountFromDb(String data, String table, String column) {
        String sqlSelect = "SELECT COUNT(*) FROM " + table + " WHERE " + column + "='" + data + "'";
        Statement stmt;
        try {
            java.sql.Connection connect = DriverManager.
                    getConnection("jdbc:mysql://localhost/search_engine", "root", "pass");
            stmt = connect.createStatement();
            return stmt.executeQuery(sqlSelect);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
// https://www.playback.ru/pickup.html
    private ResultSet selectAllFromDb(String table) {
        String sqlSelect = "SELECT * FROM " + table;
        Statement stmt;
        try {
            java.sql.Connection connect = DriverManager.
                    getConnection("jdbc:mysql://localhost/search_engine", "root", "pass");
            stmt = connect.createStatement();
            return stmt.executeQuery(sqlSelect);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
