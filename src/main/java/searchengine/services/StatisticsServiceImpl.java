package searchengine.services;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import searchengine.config.SitesList;
import searchengine.dto.db.DbCommands;
import searchengine.dto.statistics.DetailedStatisticsItem;
import searchengine.dto.statistics.StatisticsData;
import searchengine.dto.statistics.StatisticsResponse;
import searchengine.dto.statistics.TotalStatistics;
import searchengine.model.SiteEntity;
import searchengine.model.Status;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import static java.time.Instant.ofEpochMilli;

@Service
@RequiredArgsConstructor
public class StatisticsServiceImpl implements StatisticsService {

    private final SitesList sites;

    private final DbCommands dbCommands = new DbCommands();

    public StatisticsResponse getStatistics() {
        TotalStatistics total = new TotalStatistics();
        total.setSites(sites.getSites().size());
        total.setIndexing(true);

        List<DetailedStatisticsItem> detailed = new ArrayList<>();
        ResultSet allSitesResultSet = dbCommands.selectAllFromDb("site");
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

        int totalPages = 0;
        int totalLemmas = 0;
        for (SiteEntity site : allSites) {
            DetailedStatisticsItem item = new DetailedStatisticsItem();
            item.setUrl(site.getUrl());
            item.setName(site.getName());
            item.setStatus(site.getStatus().name());
            item.setStatusTime(site.getStatusTime().atZone(ZoneId.systemDefault()).toEpochSecond());
            if (!site.getLastError().isEmpty()) {
                item.setError(site.getLastError());
            } else {
                item.setError("");
            }
            ResultSet resultSetFromPageTable = dbCommands.selectCountWithParameter("page",
                    "site_id_id", String.valueOf(site.getId()));
            try {
                if (resultSetFromPageTable.next()) {
                    int pagesCount = resultSetFromPageTable.getInt("COUNT(*)");
                    totalPages += pagesCount;
                    item.setPages(pagesCount);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            ResultSet resultSetFromLemmaTable = dbCommands.selectCountWithParameter("lemma",
                    "site_id_id", String.valueOf(site.getId()));
            try {
                if (resultSetFromLemmaTable.next()) {
                    int lemmasCount = resultSetFromLemmaTable.getInt("COUNT(*)");
                    totalLemmas += lemmasCount;
                    item.setLemmas(lemmasCount);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            detailed.add(item);
        }
        total.setPages(totalPages);
        total.setLemmas(totalLemmas);

        StatisticsResponse response = new StatisticsResponse();
        StatisticsData data = new StatisticsData();
        data.setTotal(total);
        data.setDetailed(detailed);
        response.setStatistics(data);
        response.setResult(true);
        return response;
    }
}
