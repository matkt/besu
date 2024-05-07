package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.diffbased.common.worldview.accumulator.DiffBasedWorldStateUpdateAccumulator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class TransactionConflictChecker {

  private final List<TransactionWithLocation> parallelizedTransactions = new ArrayList<>();

  private final DiffBasedWorldStateUpdateAccumulator<?>[] accumulatorByParallelizedTransaction;

  private final TransactionProcessingResult[] resultByParallelizedTransaction;

  public TransactionConflictChecker(final int nbTrx) {
    this.accumulatorByParallelizedTransaction = new DiffBasedWorldStateUpdateAccumulator<?>[nbTrx];
    this.resultByParallelizedTransaction = new TransactionProcessingResult[nbTrx];
  }

  public void findParallelTransactions(
      final Address producer, final List<Transaction> transactions) {
    for (int i = 0; i < transactions.size(); i++) {
      Transaction tx1 = transactions.get(i);
      boolean conflict = false;
      if (!tx1.getSender().equals(producer)
          && (tx1.getTo().isEmpty() || !tx1.getTo().get().equals(producer))) {
        for (int j = 0; j < i; j++) {
          Transaction tx2 = transactions.get(j);
          conflict =
              tx1.getSender().equals(tx2.getSender())
                  || (tx2.getTo().isPresent()
                      && tx1.getTo().isPresent()
                      && tx1.getTo().get().equals(tx2.getTo().get()))
                  || (tx2.getTo().isPresent() && tx1.getSender().equals(tx2.getTo().get()))
                  || (tx1.getTo().isPresent() && tx1.getTo().get().equals(tx2.getSender()));
          if (conflict) {
            break;
          }
        }
        if (!conflict) {
          parallelizedTransactions.add(new TransactionWithLocation(i, tx1));
        }
      }
    }

    parallelizedTransactions.sort(
        (o1, o2) -> Long.compare(o2.transaction.getGasLimit(), o1.transaction.getGasLimit()));

    System.out.println("findParallelTransactions end " + parallelizedTransactions.size());
  }

  public void saveParallelizedTransactionProcessingResult(
      final TransactionWithLocation transaction,
      final DiffBasedWorldStateUpdateAccumulator<?> accumulator,
      final TransactionProcessingResult result) {
    accumulatorByParallelizedTransaction[transaction.getLocation()] = accumulator;
    resultByParallelizedTransaction[transaction.getLocation()] = result;
  }

  public boolean checkConflicts(
      final Address producer,
      final TransactionWithLocation transaction,
      final DiffBasedWorldStateUpdateAccumulator<?> trxAccumulator,
      final DiffBasedWorldStateUpdateAccumulator<?> blockAccumulator) {

    final Set<Address> addressesTouchedByTransaction =
        getAddressesTouchedByTransaction(transaction, Optional.of(trxAccumulator));
    if (addressesTouchedByTransaction.contains(producer)) {
      return true;
    }

    List<Address> addressesTouchByBlock = new ArrayList<>();
    blockAccumulator
        .getAccountsToUpdate()
        .forEach(
            (address, diffBasedValue) -> {
              if (!diffBasedValue.isUnchanged()) {
                addressesTouchByBlock.add(address);
              }
            });

    final List<Address> commonAddresses = new ArrayList<>(addressesTouchedByTransaction);
    commonAddresses.retainAll(addressesTouchByBlock);
    return !commonAddresses.isEmpty();
  }

  public List<Address> getNoConflictsData(
      final Address producer,
      final TransactionWithLocation transaction,
      final DiffBasedWorldStateUpdateAccumulator<?> trxAccumulator,
      final DiffBasedWorldStateUpdateAccumulator<?> blockAccumulator) {
    final Set<Address> addressesTouchedByTransaction =
        getAddressesTouchedByTransaction(transaction, Optional.of(trxAccumulator));
    final List<Address> safeAddresses = new ArrayList<>(addressesTouchedByTransaction);
    safeAddresses.removeAll(blockAccumulator.getAccountsToUpdate().keySet());
    safeAddresses.remove(producer);
    return safeAddresses;
  }

  private Set<Address> getAddressesTouchedByTransaction(
      final TransactionWithLocation transaction,
      final Optional<DiffBasedWorldStateUpdateAccumulator<?>> accumulator) {

    HashSet<Address> addresses = new HashSet<>();
    addresses.add(transaction.getSender());
    if (transaction.getTo().isPresent()) {
      addresses.add(transaction.getTo().get());
    }
    accumulator.ifPresent(
        diffBasedWorldStateUpdateAccumulator ->
            addresses.addAll(diffBasedWorldStateUpdateAccumulator.getAccountsToUpdate().keySet()));
    return addresses;
  }

  public List<TransactionWithLocation> getParallelizedTransactions() {
    return parallelizedTransactions;
  }

  public DiffBasedWorldStateUpdateAccumulator<?>[] getAccumulatorByTransaction() {
    return accumulatorByParallelizedTransaction;
  }

  public TransactionProcessingResult[] getResultByTransaction() {
    return resultByParallelizedTransaction;
  }

  public static final class TransactionWithLocation {
    private final int location;
    private final Transaction transaction;

    public TransactionWithLocation(final int location, final Transaction transaction) {
      this.location = location;
      this.transaction = transaction;
    }

    public int getLocation() {
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
      return location == that.location;
    }

    @Override
    public int hashCode() {
      return Objects.hash(location);
    }
  }
}
