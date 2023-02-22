package org.hyperledger.besu.ethereum.zkevm;

import java.util.Optional;

public class ZkValue<T> {
  public Optional<T> getPreimage() {
    return preimage;
  }

  public void setPreimage(Optional<T> preimage) {
    this.preimage = preimage;
  }

  public Optional<T> getPostImage() {
    return postImage;
  }

  public void setPostImage(Optional<T> postImage) {
    this.postImage = postImage;
  }

  private Optional<T> preimage;
  private Optional<T> postImage;

  public ZkValue(final Optional<T> preimage, final Optional<T> postImage) {
    this.preimage = preimage;
    this.postImage = postImage;
  }
}
