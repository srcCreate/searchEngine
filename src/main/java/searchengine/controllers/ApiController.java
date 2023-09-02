package searchengine.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import searchengine.dto.search.SearchingResponse;
import searchengine.dto.statistics.StatisticsResponse;
import searchengine.model.PageRepository;
import searchengine.services.IndexingPageService;
import searchengine.services.IndexingService;
import searchengine.services.SearchingService;
import searchengine.services.StatisticsService;

import java.util.Map;

@RestController
@RequestMapping("/api")
public class ApiController {

    @Autowired
    private StatisticsService statisticsService;

    @Autowired
    private IndexingService indexingService;

    @Autowired
    private IndexingPageService indexingPageService;

    @Autowired
    private PageRepository pageRepository;

    @Autowired
    private SearchingService searchingService;

    @GetMapping("/statistics")
    public ResponseEntity<StatisticsResponse> statistics() {
        return ResponseEntity.ok(statisticsService.getStatistics());
    }

    @GetMapping("/startindexing")
    public void startIndexing() {
        ResponseEntity.ok(indexingService.startIndexing());
    }

    @GetMapping("/stopindexing")
    public void stopIndexing() {
        ResponseEntity.ok(indexingService.stopIndexing());
    }

    @PostMapping(value = "/indexPage")
    public Map<String, String> indexPage(@RequestParam String url) {
        return indexingPageService.indexPage(url);
    }

    @GetMapping(value = "/search")
    public SearchingResponse search(@RequestParam String query, String site) {
        return searchingService.search(query, site);
    }
}