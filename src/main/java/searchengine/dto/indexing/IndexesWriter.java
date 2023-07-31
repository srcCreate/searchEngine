package searchengine.dto.indexing;

import searchengine.model.IndexEntity;
import searchengine.model.IndexRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

public class IndexesWriter extends RecursiveAction {

    IndexRepository indexRepository;

    private List<IndexEntity> targetCollection;

    public IndexesWriter(List<IndexEntity> targetCollection, IndexRepository lemmaRepository) {
        this.targetCollection = targetCollection;
        this.indexRepository = lemmaRepository;
    }

    @Override
    protected void compute() {
        System.out.println("Start compute IndexesWriter in tread: " + Thread.currentThread().getName());
        if (targetCollection.size() > 4) {
            ForkJoinTask.invokeAll(createSubTasks());
        } else {
            targetCollection.forEach(indexRepository::save);
        }
        System.out.println("STOP compute IndexesWriter in tread: " + Thread.currentThread().getName());
    }

    private List<IndexesWriter> createSubTasks() {
        List<IndexesWriter> subTasks = new ArrayList<>();
        IndexesWriter firstHalfTargetCollection = new IndexesWriter(
                targetCollection.subList(0,targetCollection.size() / 2), indexRepository);
        IndexesWriter secondHalfTargetCollection = new IndexesWriter(
                targetCollection.subList(targetCollection.size() / 2, targetCollection.size() - 1), indexRepository);

        subTasks.add(firstHalfTargetCollection);
        subTasks.add(secondHalfTargetCollection);

        return subTasks;
    }
}
