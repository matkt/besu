package org.hyperledger.besu.ethereum.trie.sparse;

import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.NodeFactory;

import java.io.ByteArrayOutputStream;
import java.lang.ref.WeakReference;
import java.util.function.Function;

public class LeafNode<V>  extends org.hyperledger.besu.ethereum.trie.patricia.LeafNode<V> {
    public LeafNode(final Bytes location, final Bytes path, final V value, final NodeFactory<V> nodeFactory, final Function<V, Bytes> valueSerializer) {
        super(location, path, value, nodeFactory, valueSerializer);
    }

    public LeafNode(final Bytes path, final V value, final NodeFactory<V> nodeFactory, final Function<V, Bytes> valueSerializer) {
        super(path, value, nodeFactory, valueSerializer);
    }

    @Override
    public Bytes getEncodedBytes() {
        if (encodedBytes != null) {
            final Bytes encoded = encodedBytes.get();
            if (encoded != null) {
                return encoded;
            }
        }

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(valueSerializer.apply(value).toArrayUnsafe());
        final Bytes encoded = Bytes.wrap(out.toByteArray());
        encodedBytes = new WeakReference<>(encoded);
        return encoded;
    }

    @Override
    public boolean isReferencedByHash() {
        return false;
    }
}
