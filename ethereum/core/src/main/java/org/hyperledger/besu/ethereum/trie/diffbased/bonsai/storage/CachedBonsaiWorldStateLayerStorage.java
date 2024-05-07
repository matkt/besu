package org.hyperledger.besu.ethereum.trie.diffbased.bonsai.storage;

import org.hyperledger.besu.services.kvstore.CacherLayeredKeyValueStorage;
import org.hyperledger.besu.services.kvstore.LayeredKeyValueStorage;

public class CachedBonsaiWorldStateLayerStorage extends BonsaiWorldStateLayerStorage{
    public CachedBonsaiWorldStateLayerStorage(final BonsaiWorldStateKeyValueStorage parent) {
        super(
                new CacherLayeredKeyValueStorage(parent.getComposedWorldStateStorage()),
                parent.getTrieLogStorage(),
                parent);
    }


}
