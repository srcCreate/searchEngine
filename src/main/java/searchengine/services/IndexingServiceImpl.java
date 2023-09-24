package searchengine.services;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import searchengine.config.SitesList;
import searchengine.dto.indexing.SiteIndexer;
import searchengine.model.*;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class IndexingServiceImpl implements IndexingService {

    private final SitesList sites;
    @Autowired
    private SiteRepository siteRepository;
    @Autowired
    private PageRepository pageRepository;

    @Autowired
    private final LemmaRepository lemmaRepository;

    @Autowired
    private final IndexRepository indexRepository;

    private final List<SiteIndexer> siteIndexerList = new ArrayList<>();

    @Override
    public SiteIndexer startIndexing() {
        long start = System.currentTimeMillis();

        sites.getSites().forEach(site -> {
            SiteIndexer siteIndexer = new SiteIndexer(site,siteRepository, pageRepository, lemmaRepository, indexRepository,
                    false);
            siteIndexerList.add(siteIndexer);
            siteIndexer.start();
        });

        for (SiteIndexer siteIndexer : siteIndexerList) {
            try {
                siteIndexer.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        System.out.println("Индексация сайтов завершена за " + (System.currentTimeMillis() - start) + " миллисекунд");
        return null;
    }

    @Override
    public SiteIndexer stopIndexing() {
        siteIndexerList.forEach(siteIndexer -> siteIndexer.getPool().shutdown());

        System.out.println("Индексация завершена по требованию пользователя");
        return null;
    }
}
