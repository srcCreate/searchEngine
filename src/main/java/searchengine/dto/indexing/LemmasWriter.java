package searchengine.dto.indexing;

import searchengine.model.LemmaEntity;
import searchengine.model.LemmaRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

public class LemmasWriter extends RecursiveAction {

    LemmaRepository lemmaRepository;

    private List<LemmaEntity> targetCollection;

    public LemmasWriter(List<LemmaEntity> targetCollection, LemmaRepository lemmaRepository) {
        this.targetCollection = targetCollection;
        this.lemmaRepository = lemmaRepository;
    }

    @Override
    protected void compute() {
        System.out.println("START compute LemmasWriter in tread: " + Thread.currentThread().getName());
        if (targetCollection.size() > 4) {
            ForkJoinTask.invokeAll(createSubTasks());
        } else {
            targetCollection.forEach(lemmaRepository::save);
        }
        System.out.println("STOP compute LemmasWriter in tread: " + Thread.currentThread().getName());
    }

    private List<LemmasWriter> createSubTasks() {
        List<LemmasWriter> subTasks = new ArrayList<>();
        LemmasWriter firstHalfTargetCollection = new LemmasWriter(
                targetCollection.subList(0,targetCollection.size() / 2), lemmaRepository);
        LemmasWriter secondHalfTargetCollection = new LemmasWriter(
                targetCollection.subList(targetCollection.size() / 2, targetCollection.size() - 1), lemmaRepository);

        subTasks.add(firstHalfTargetCollection);
        subTasks.add(secondHalfTargetCollection);

        return subTasks;
    }
}
