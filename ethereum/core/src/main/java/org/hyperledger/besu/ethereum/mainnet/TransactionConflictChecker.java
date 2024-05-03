package org.hyperledger.besu.ethereum.mainnet;


import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.diffbased.common.worldview.accumulator.DiffBasedWorldStateUpdateAccumulator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionConflictChecker {

    private final List<TransactionWithLocation> parallelizedTransactions = Collections.synchronizedList(new ArrayList<>());

    private final List<TransactionWithLocation> processedTransactions = Collections.synchronizedList(new ArrayList<>());

    private final Map<Long, DiffBasedWorldStateUpdateAccumulator<?>> accumulatorByTransaction = new ConcurrentHashMap<>();

    private final Map<Long, TransactionProcessingResult> resultByTransaction = new ConcurrentHashMap<>();

    public void organizeTransactionsIntoRounds(final List<Transaction> transactions){
        System.out.println("organizeTransactionsIntoRounds start");
        for (int i = 0; i < transactions.size(); i++) {
            Transaction tx1 = transactions.get(i);
            boolean conflict = false;
            for (int j = 0; j < i; j++) {
                Transaction tx2 = transactions.get(j);
                conflict=tx1.getSender().equals(tx2.getSender()) ||
                        (tx2.getTo().isPresent() && tx1.getTo().isPresent() && tx1.getTo().get().equals(tx2.getTo().get())) ||
                        (tx2.getTo().isPresent() && tx1.getSender().equals(tx2.getTo().get())) ||
                        (tx1.getTo().isPresent() && tx1.getTo().get().equals(tx2.getSender()));
                if(conflict) {
                    break;
                }
            }
            if (!conflict) {
                parallelizedTransactions.add(new TransactionWithLocation(i, tx1));
            }
        }

        System.out.println("organizeTransactionsIntoRounds end "+parallelizedTransactions.size());
    }

    public void saveTransactionProcessingResult(final TransactionWithLocation transaction, final DiffBasedWorldStateUpdateAccumulator<?> accumulator, final TransactionProcessingResult result){
        processedTransactions.add(transaction);
        accumulatorByTransaction.put(transaction.getLocation(),accumulator);
        resultByTransaction.put(transaction.getLocation(), result);
    }

    public void checkAndResolveConflicts(final List<Transaction> transactions) {
        // Temporary list to hold transactions that need to be moved to sequentialTransactions due to conflicts
        List<TransactionWithLocation> toBeMoved = new ArrayList<>();

        for (int i = 0; i < parallelizedTransactions.size(); i++) {
            TransactionWithLocation tx1 = parallelizedTransactions.get(i);
            for (int j = 0; j < tx1.getLocation(); j++) {
                TransactionWithLocation tx2 = new TransactionWithLocation(j,transactions.get(j));
                if (tx1.getLocation() != tx2.getLocation()) {
                    Optional<DiffBasedWorldStateUpdateAccumulator<?>> accumulator1 = Optional.ofNullable(accumulatorByTransaction.get(tx1.getLocation()));
                    Set<Address> addresses1 = getAddressesTouchedByTransaction(tx1, accumulator1);

                    Optional<DiffBasedWorldStateUpdateAccumulator<?>> accumulator2 = Optional.ofNullable(accumulatorByTransaction.get(tx2.getLocation()));
                    Set<Address> addresses2 = getAddressesTouchedByTransaction(tx2, accumulator2);

                    // Check for common addresses indicating a conflict
                    Set<Address> commonAddresses = new HashSet<>(addresses1);
                    commonAddresses.retainAll(addresses2);

                    if (!commonAddresses.isEmpty()) {
                        // Conflict detected, determine which transaction has the higher nonce
                        TransactionWithLocation toMove = tx1.getLocation() > tx2.getLocation() ? tx1 : tx2;

                        if (!toBeMoved.contains(toMove)) {
                            //System.out.println("conflict detected " +toMove.transaction.getHash()+" "+tx1.transaction.getHash() + " " + tx2.transaction.getHash() + " " + tx1.location + " " + tx2.location + " " + commonAddresses);
                            toBeMoved.add(toMove);
                        }
                    }
                }
            }
        }

        // Move the conflicting transactions to sequentialTransactions and remove them from processedTransactions and accumulatorMap
        for (TransactionWithLocation tx : toBeMoved) {
            processedTransactions.remove(tx);
            accumulatorByTransaction.remove(tx.getLocation());
            resultByTransaction.remove(tx.getLocation());
        }

    }

    private Set<Address> getAddressesTouchedByTransaction(final TransactionWithLocation transaction, final Optional<DiffBasedWorldStateUpdateAccumulator<?>> accumulator) {
        HashSet<Address> addresses = new HashSet<>();
        addresses.add(transaction.getSender());
        if(transaction.getTo().isPresent()){
            addresses.add(transaction.getTo().get());
        }
        accumulator.ifPresent(diffBasedWorldStateUpdateAccumulator ->
                addresses.addAll(diffBasedWorldStateUpdateAccumulator.getAccountsToUpdate().keySet()));
        return addresses;
    }

    public List<TransactionWithLocation> getParallelizedTransactions() {
        return parallelizedTransactions;
    }

    public List<TransactionWithLocation> getProcessedTransactions() {
        return processedTransactions;
    }

    public Map<Long, DiffBasedWorldStateUpdateAccumulator<?>> getAccumulatorByTransaction() {
        return accumulatorByTransaction;
    }

    public Map<Long, TransactionProcessingResult> getResultByTransaction() {
        return resultByTransaction;
    }

    public static final class TransactionWithLocation {
        private final long location;
        private final Transaction transaction;

        public TransactionWithLocation(final long location, final Transaction transaction) {
            this.location = location;
            this.transaction = transaction;
        }

        public long getLocation() {
            return location;
        }

        public Transaction transaction() {
            return transaction;
        }

        public Address getSender() {
            return transaction.getSender();
        }

        public Optional<Address> getTo() {
            return transaction.getTo();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TransactionWithLocation that = (TransactionWithLocation) o;
            return location == that.location ;
        }

        @Override
        public int hashCode() {
            return Objects.hash(location);
        }
    }
}
