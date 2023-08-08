package searchengine.services;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import searchengine.config.SitesList;
import searchengine.dto.indexing.SiteIndexer;
import searchengine.model.*;

import java.util.concurrent.ForkJoinPool;

@Service
@RequiredArgsConstructor
public class IndexingServiceImpl implements IndexingService {

    private final SitesList sites;

    private final SiteRepository siteRepository;
    @Autowired
    private PageRepository pageRepository;

    @Autowired
    private final LemmaRepository lemmaRepository;

    @Autowired
    private final IndexRepository indexRepository;

    private volatile boolean isStopped = false;

    private ForkJoinPool pool;
    private SiteIndexer task;
    @Override
    public SiteIndexer startIndexing() {
        long start = System.currentTimeMillis();

        task = new SiteIndexer(sites.getSites(), siteRepository, pageRepository, lemmaRepository, indexRepository, isStopped);
        pool = ForkJoinPool.commonPool();
        pool.invoke(task);
        System.out.println("Индексация сайтов завершена за " + (System.currentTimeMillis() - start) + " миллисекунд");
        return null;
    }

    @Override
    public SiteIndexer stopIndexing() {
        isStopped = true;
        if (pool != null) {
            pool.shutdown();
        }

        System.out.println("Индексация завершена по требованию пользователя");
        return null;
    }
}
