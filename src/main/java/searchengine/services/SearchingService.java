package searchengine.services;

import searchengine.dto.search.SearchingResponse;

import java.util.Map;

public interface SearchingService {
    SearchingResponse search(String query);
}
