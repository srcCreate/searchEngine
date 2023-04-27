package searchengine.services;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import searchengine.config.Site;
import searchengine.config.SitesList;
import searchengine.dto.indexing.PageIndexer;
import searchengine.dto.indexing.SiteIndexer;
import searchengine.model.*;

import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;

@Service
@RequiredArgsConstructor
public class IndexingServiceImpl implements IndexingService {

    private final SitesList sites;

    private final SiteRepository siteRepository;
    private final PageRepository pageRepository;

    @Override
    public PageIndexer startIndexing() {
        int numFlow = Runtime.getRuntime().availableProcessors() - 1;

        long start = System.currentTimeMillis();

        SiteIndexer task = new SiteIndexer(sites.getSites(), siteRepository, pageRepository);
        ForkJoinPool.commonPool().invoke(task);

        System.out.println("Индексация сайтов завершена за " + (System.currentTimeMillis() - start) + " миллисекунд");

        return null;
    }
}
