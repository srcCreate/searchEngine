package searchengine.services;

import searchengine.dto.search.SearchingResponse;

public interface SearchingService {
    SearchingResponse search(String query, String site);
}
