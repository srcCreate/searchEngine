package searchengine.services;

import searchengine.dto.indexing.PageIndexer;

public interface IndexingService {
    PageIndexer startIndexing();
}
