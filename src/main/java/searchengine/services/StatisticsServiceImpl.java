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

import java.sql.Connection;
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
        List<SiteEntity> allSites = new ArrayList<>();


        try (Connection connection = dbCommands.getNewConnection()) {
            String sqlSelect = "SELECT * FROM site";
            ResultSet rs = connection.createStatement().executeQuery(sqlSelect);
            while (rs.next()) {
                SiteEntity site = new SiteEntity();
                site.setId(rs.getInt("id"));
                site.setLastError(rs.getString("last_error"));
                site.setName(rs.getString("name"));
                site.setStatus(Status.valueOf(rs.getString("status")));
                long timestamp = rs.getTimestamp("status_time").getTime();
                site.setStatusTime(LocalDateTime.ofInstant(ofEpochMilli(timestamp), TimeZone.getDefault().toZoneId()));
                site.setUrl(rs.getString("url"));
                allSites.add(site);
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

                String selectCountFromPage = "SELECT COUNT(*) FROM page WHERE site_id_id ='" + site.getId() + "'";
                ResultSet pageRs = connection.createStatement().executeQuery(selectCountFromPage);
                if (pageRs.next()) {
                    int pagesCount = pageRs.getInt("COUNT(*)");
                    totalPages += pagesCount;
                    item.setPages(pagesCount);
                }

                String selectCountFromLemma = "SELECT COUNT(*) FROM lemma WHERE site_id_id ='" + site.getId() + "'";
                ResultSet lemmaRs = connection.createStatement().executeQuery(selectCountFromLemma);
                if (lemmaRs.next()) {
                    int lemmasCount = lemmaRs.getInt("COUNT(*)");
                    totalLemmas += lemmasCount;
                    item.setLemmas(lemmasCount);
                }
                detailed.add(item);
            }
            total.setPages(totalPages);
            total.setLemmas(totalLemmas);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        StatisticsResponse response = new StatisticsResponse();
        StatisticsData data = new StatisticsData();
        data.setTotal(total);
        data.setDetailed(detailed);
        response.setStatistics(data);
        response.setResult(true);
        return response;
    }
}
