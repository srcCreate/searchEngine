package searchengine.services;

import searchengine.dto.indexing.SiteIndexer;

public interface IndexingService {
    SiteIndexer startIndexing();
    SiteIndexer stopIndexing();
}
