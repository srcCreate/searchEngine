package searchengine.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import searchengine.dto.indexing.PageIndexer;
import searchengine.dto.statistics.StatisticsResponse;
import searchengine.model.PageRepository;
import searchengine.services.IndexingService;
import searchengine.services.StatisticsService;

@RestController
@RequestMapping("/api")
public class ApiController {

    private final StatisticsService statisticsService;

    @Autowired
    private final IndexingService indexingService;

    private final PageRepository pageRepository;

    public ApiController(StatisticsService statisticsService, IndexingService indexingService, PageRepository pageRepository) {
        this.statisticsService = statisticsService;
        this.indexingService = indexingService;
        this.pageRepository = pageRepository;
    }

    @GetMapping("/statistics")
    public ResponseEntity<StatisticsResponse> statistics() {
        return ResponseEntity.ok(statisticsService.getStatistics());
    }

    // write method
    @GetMapping("/startIndexing")
    public void startIndexing() {
        ResponseEntity.ok(indexingService.startIndexing());
    }
}