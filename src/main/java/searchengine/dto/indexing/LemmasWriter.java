package searchengine.dto.indexing;

import searchengine.dto.db.DbCommands;
import searchengine.model.LemmaEntity;
import searchengine.model.LemmaRepository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

public class LemmasWriter extends RecursiveAction {

    LemmaRepository lemmaRepository;

    private final DbCommands dbCommands = new DbCommands();

    private List<LemmaEntity> targetCollection;

    public LemmasWriter(List<LemmaEntity> targetCollection, LemmaRepository lemmaRepository) {
        this.targetCollection = targetCollection;
        this.lemmaRepository = lemmaRepository;
    }

    @Override
    protected void compute() {
        if (targetCollection.size() > 4) {
            ForkJoinTask.invokeAll(createSubTasks());
        } else {
            targetCollection.forEach(lemmaEntity -> {
                try {
                    ResultSet rs = dbCommands.selectAllFromDb("lemma", "lemma", lemmaEntity.getLemma());
                    // Проверка на повторение леммы на других сайтах, увеличение frequency
                    if (rs.next() && (rs.getInt("site_id_id") != lemmaEntity.getSiteId().getId())) {
                        int currentFrequency = lemmaEntity.getFrequency();
                        lemmaEntity.setFrequency(currentFrequency++);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private List<LemmasWriter> createSubTasks() {
        List<LemmasWriter> subTasks = new ArrayList<>();
        LemmasWriter firstHalfTargetCollection = new LemmasWriter(
                targetCollection.subList(0, targetCollection.size() / 2), lemmaRepository);
        LemmasWriter secondHalfTargetCollection = new LemmasWriter(
                targetCollection.subList(targetCollection.size() / 2, targetCollection.size() - 1), lemmaRepository);

        subTasks.add(firstHalfTargetCollection);
        subTasks.add(secondHalfTargetCollection);

        return subTasks;
    }
}
