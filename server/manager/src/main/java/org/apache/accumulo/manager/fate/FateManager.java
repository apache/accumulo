package org.apache.accumulo.manager.fate;

import com.google.common.net.HostAndPort;
import org.apache.accumulo.core.fate.FateId;
import org.apache.hadoop.util.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FateManager {

    record FatePartition(FateId start, FateId end) {

    }

    public void managerWorkers() throws InterruptedException {
        while(true) {
            // TODO make configurable
            Thread.sleep(10_000);

            // TODO could support RG... could user ServerId
            // This map will contain all current workers even their partitions are empty
            Map<HostAndPort, Set<FatePartition>> currentAssignments = getCurrentAssignments();
            Set<FatePartition> desiredParititions = getDesiredPartitions();

            // TODO handle duplicate current assignments

            Map<HostAndPort, Set<FatePartition>> desired = computeDesiredAssignments(currentAssignments, desiredParititions);

            // are there any workers with extra partitions?  If so need to unload those first.
            boolean haveExtra = desired.entrySet().stream().anyMatch(e->{
                HostAndPort worker = e.getKey();
                var curr = currentAssignments.getOrDefault(worker, Set.of());
                var extra = Sets.difference(curr, e.getValue());
                return !extra.isEmpty();
            });

            if(haveExtra) {
                // force unload of extra partitions to make them available for other workers
                desired.forEach((worker, paritions)->{
                    var curr = currentAssignments.getOrDefault(worker, Set.of());
                    if(!curr.equals(paritions)) {
                        var intersection = Sets.intersection(curr, paritions);
                        setWorkerPartitions(worker, intersection);
                        currentAssignments.put(worker, intersection);
                    }
                });
            }

            // Load all partitions on all workers..
            desired.forEach((worker, paritions)->{
                var curr = currentAssignments.getOrDefault(worker, Set.of());
                if(!curr.equals(paritions)){
                    setWorkerPartitions(worker, paritions);
                }
            });
        }
    }

    private void setWorkerPartitions(HostAndPort worker, Set<FatePartition> partitions) {
        // TODO make RPC to get update nonce
        // TODO update partitions using nonce
    }

    /**
     * Compute the desired distribution of partitions across workers.  Favors leaving partitions in place if possible.
     */
    private Map<HostAndPort, Set<FatePartition>> computeDesiredAssignments(Map<HostAndPort, Set<FatePartition>> currentAssignments, Set<FatePartition> desiredParititions) {
        // min number of partitions a single worker must have
        int minPerWorker = currentAssignments.size() / desiredParititions.size();
        // max number of partitions a single worker can have
        int maxPerWorker = minPerWorker + Math.min(currentAssignments.size() % desiredParititions.size(), 1);
        // number of workers that can have the max partitions
        int desiredWorkersWithMax = currentAssignments.size() % desiredParititions.size();

        Map<HostAndPort, Set<FatePartition>> desiredAssignments = new HashMap<>();
        Set<FatePartition> availablePartitions = new HashSet<>(desiredParititions);

        // remove everything that is assigned
        currentAssignments.values().forEach(p->p.forEach(availablePartitions::remove));

        // Find workers that currently have too many partitions assigned and place their excess in the available set.  Let workers keep what they have when its under the limit.
        int numWorkersWithMax = 0;
        for(var worker : currentAssignments.keySet()) {
            var assignments = new HashSet<FatePartition>();
            var curr = currentAssignments.getOrDefault(worker, Set.of());
            // The number of partitions this worker can have, anything in excess should be added to available
            int canHave = numWorkersWithMax < desiredWorkersWithMax ? maxPerWorker : minPerWorker;

            var iter = curr.iterator();
            for(int i = 0; i<canHave && iter.hasNext();i++){
                assignments.add(iter.next());
            }
            iter.forEachRemaining(availablePartitions::add);

            desiredAssignments.put(worker, assignments);
            if(curr.size() >= maxPerWorker) {
                numWorkersWithMax++;
            }
        }

        // Distribute available partitions to workers that do not have the minimum.
        var availIter = availablePartitions.iterator();
        for(var worker : currentAssignments.keySet()) {
            var assignments = desiredAssignments.get(worker);
            while(assignments.size() < minPerWorker) {
                // This should always have next if the creation of available partitions was done correctly.
                assignments.add(availIter.next());
            }
        }

        // Distribute available partitions to workers that do not have the max until no more partitions available.
        for(var worker : currentAssignments.keySet()) {
            var assignments = desiredAssignments.get(worker);
            while(assignments.size() < maxPerWorker && availIter.hasNext()){
                assignments.add(availIter.next());
            }
            if(!availIter.hasNext()){
                break;
            }
        }

        return desiredAssignments;
    }

    private Set<FatePartition> getDesiredPartitions() {
        throw new UnsupportedOperationException();
    }

    private Map<HostAndPort, Set<FatePartition>> getCurrentAssignments() {
        throw new UnsupportedOperationException();
    }



    // TODO this will not need a main eventually, will be run by the manager
    public static void main(String[] args) {

    }
}
