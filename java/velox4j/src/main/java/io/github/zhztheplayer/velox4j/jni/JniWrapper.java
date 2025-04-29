package io.github.zhztheplayer.velox4j.jni;

import com.google.common.annotations.VisibleForTesting;

import io.github.zhztheplayer.velox4j.iterator.DownIterator;

final class JniWrapper {
  private final long sessionId;

  JniWrapper(long sessionId) {
    this.sessionId = sessionId;
  }

  @CalledFromNative
  public long sessionId() {
    return sessionId;
  }

  // Expression evaluation.
  native long createEvaluator(String evalJson);

  native long evaluatorEval(long evaluatorId, long selectivityVectorId, long rvId);

  // Plan execution.
  native long createQueryExecutor(String queryJson);

  native long queryExecutorExecute(long id);

  // For UpIterator.
  native long upIteratorGet(long id);

  // For DownIterator.
  native long createExternalStreamFromDownIterator(DownIterator itr);

  native long createBlockingQueue();

  // For BaseVector / RowVector / SelectivityVector.
  native long createEmptyBaseVector(String typeJson);

  native long arrowToBaseVector(long cSchema, long cArray);

  native long[] baseVectorDeserialize(String serialized);

  native long baseVectorWrapInConstant(long id, int length, int index);

  native long baseVectorSlice(long id, int offset, int length);

  native long baseVectorLoadedVector(long id);

  native long createSelectivityVector(int length);

  // For TableWrite.
  native String tableWriteTraitsOutputTypeWithAggregationNode(String aggregationNodeJson);

  // For serde.
  native long iSerializableAsCpp(String json);

  native long variantAsCpp(String json);

  @VisibleForTesting
  native long createUpIteratorWithExternalStream(long id);
}
