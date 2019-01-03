// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: job_master.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.RunPRequest}
 */
public  final class RunPRequest extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.RunPRequest)
    RunPRequestOrBuilder {
private static final long serialVersionUID = 0L;
  // Use RunPRequest.newBuilder() to construct.
  private RunPRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private RunPRequest() {
    jobConfig_ = com.google.protobuf.ByteString.EMPTY;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private RunPRequest(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    this();
    if (extensionRegistry == null) {
      throw new java.lang.NullPointerException();
    }
    int mutable_bitField0_ = 0;
    com.google.protobuf.UnknownFieldSet.Builder unknownFields =
        com.google.protobuf.UnknownFieldSet.newBuilder();
    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          default: {
            if (!parseUnknownField(
                input, unknownFields, extensionRegistry, tag)) {
              done = true;
            }
            break;
          }
          case 10: {
            bitField0_ |= 0x00000001;
            jobConfig_ = input.readBytes();
            break;
          }
          case 18: {
            alluxio.grpc.RunPOptions.Builder subBuilder = null;
            if (((bitField0_ & 0x00000002) == 0x00000002)) {
              subBuilder = options_.toBuilder();
            }
            options_ = input.readMessage(alluxio.grpc.RunPOptions.PARSER, extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(options_);
              options_ = subBuilder.buildPartial();
            }
            bitField0_ |= 0x00000002;
            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new com.google.protobuf.InvalidProtocolBufferException(
          e).setUnfinishedMessage(this);
    } finally {
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return alluxio.grpc.JobMasterProto.internal_static_alluxio_grpc_RunPRequest_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.JobMasterProto.internal_static_alluxio_grpc_RunPRequest_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.RunPRequest.class, alluxio.grpc.RunPRequest.Builder.class);
  }

  private int bitField0_;
  public static final int JOBCONFIG_FIELD_NUMBER = 1;
  private com.google.protobuf.ByteString jobConfig_;
  /**
   * <code>optional bytes jobConfig = 1;</code>
   */
  public boolean hasJobConfig() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>optional bytes jobConfig = 1;</code>
   */
  public com.google.protobuf.ByteString getJobConfig() {
    return jobConfig_;
  }

  public static final int OPTIONS_FIELD_NUMBER = 2;
  private alluxio.grpc.RunPOptions options_;
  /**
   * <code>optional .alluxio.grpc.RunPOptions options = 2;</code>
   */
  public boolean hasOptions() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <code>optional .alluxio.grpc.RunPOptions options = 2;</code>
   */
  public alluxio.grpc.RunPOptions getOptions() {
    return options_ == null ? alluxio.grpc.RunPOptions.getDefaultInstance() : options_;
  }
  /**
   * <code>optional .alluxio.grpc.RunPOptions options = 2;</code>
   */
  public alluxio.grpc.RunPOptionsOrBuilder getOptionsOrBuilder() {
    return options_ == null ? alluxio.grpc.RunPOptions.getDefaultInstance() : options_;
  }

  private byte memoizedIsInitialized = -1;
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      output.writeBytes(1, jobConfig_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      output.writeMessage(2, getOptions());
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.CodedOutputStream
        .computeBytesSize(1, jobConfig_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(2, getOptions());
    }
    size += unknownFields.getSerializedSize();
    memoizedSize = size;
    return size;
  }

  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof alluxio.grpc.RunPRequest)) {
      return super.equals(obj);
    }
    alluxio.grpc.RunPRequest other = (alluxio.grpc.RunPRequest) obj;

    boolean result = true;
    result = result && (hasJobConfig() == other.hasJobConfig());
    if (hasJobConfig()) {
      result = result && getJobConfig()
          .equals(other.getJobConfig());
    }
    result = result && (hasOptions() == other.hasOptions());
    if (hasOptions()) {
      result = result && getOptions()
          .equals(other.getOptions());
    }
    result = result && unknownFields.equals(other.unknownFields);
    return result;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    if (hasJobConfig()) {
      hash = (37 * hash) + JOBCONFIG_FIELD_NUMBER;
      hash = (53 * hash) + getJobConfig().hashCode();
    }
    if (hasOptions()) {
      hash = (37 * hash) + OPTIONS_FIELD_NUMBER;
      hash = (53 * hash) + getOptions().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.RunPRequest parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.RunPRequest parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.RunPRequest parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.RunPRequest parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.RunPRequest parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.RunPRequest parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.RunPRequest parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.RunPRequest parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.RunPRequest parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.RunPRequest parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.RunPRequest parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.RunPRequest parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(alluxio.grpc.RunPRequest prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @java.lang.Override
  protected Builder newBuilderForType(
      com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * Protobuf type {@code alluxio.grpc.RunPRequest}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.RunPRequest)
      alluxio.grpc.RunPRequestOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.JobMasterProto.internal_static_alluxio_grpc_RunPRequest_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.JobMasterProto.internal_static_alluxio_grpc_RunPRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.RunPRequest.class, alluxio.grpc.RunPRequest.Builder.class);
    }

    // Construct using alluxio.grpc.RunPRequest.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (com.google.protobuf.GeneratedMessageV3
              .alwaysUseFieldBuilders) {
        getOptionsFieldBuilder();
      }
    }
    public Builder clear() {
      super.clear();
      jobConfig_ = com.google.protobuf.ByteString.EMPTY;
      bitField0_ = (bitField0_ & ~0x00000001);
      if (optionsBuilder_ == null) {
        options_ = null;
      } else {
        optionsBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000002);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.JobMasterProto.internal_static_alluxio_grpc_RunPRequest_descriptor;
    }

    public alluxio.grpc.RunPRequest getDefaultInstanceForType() {
      return alluxio.grpc.RunPRequest.getDefaultInstance();
    }

    public alluxio.grpc.RunPRequest build() {
      alluxio.grpc.RunPRequest result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.RunPRequest buildPartial() {
      alluxio.grpc.RunPRequest result = new alluxio.grpc.RunPRequest(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      result.jobConfig_ = jobConfig_;
      if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
        to_bitField0_ |= 0x00000002;
      }
      if (optionsBuilder_ == null) {
        result.options_ = options_;
      } else {
        result.options_ = optionsBuilder_.build();
      }
      result.bitField0_ = to_bitField0_;
      onBuilt();
      return result;
    }

    public Builder clone() {
      return (Builder) super.clone();
    }
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return (Builder) super.setField(field, value);
    }
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return (Builder) super.clearField(field);
    }
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return (Builder) super.clearOneof(oneof);
    }
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, java.lang.Object value) {
      return (Builder) super.setRepeatedField(field, index, value);
    }
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return (Builder) super.addRepeatedField(field, value);
    }
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof alluxio.grpc.RunPRequest) {
        return mergeFrom((alluxio.grpc.RunPRequest)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.RunPRequest other) {
      if (other == alluxio.grpc.RunPRequest.getDefaultInstance()) return this;
      if (other.hasJobConfig()) {
        setJobConfig(other.getJobConfig());
      }
      if (other.hasOptions()) {
        mergeOptions(other.getOptions());
      }
      this.mergeUnknownFields(other.unknownFields);
      onChanged();
      return this;
    }

    public final boolean isInitialized() {
      return true;
    }

    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      alluxio.grpc.RunPRequest parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.RunPRequest) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private com.google.protobuf.ByteString jobConfig_ = com.google.protobuf.ByteString.EMPTY;
    /**
     * <code>optional bytes jobConfig = 1;</code>
     */
    public boolean hasJobConfig() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional bytes jobConfig = 1;</code>
     */
    public com.google.protobuf.ByteString getJobConfig() {
      return jobConfig_;
    }
    /**
     * <code>optional bytes jobConfig = 1;</code>
     */
    public Builder setJobConfig(com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      jobConfig_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional bytes jobConfig = 1;</code>
     */
    public Builder clearJobConfig() {
      bitField0_ = (bitField0_ & ~0x00000001);
      jobConfig_ = getDefaultInstance().getJobConfig();
      onChanged();
      return this;
    }

    private alluxio.grpc.RunPOptions options_ = null;
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.RunPOptions, alluxio.grpc.RunPOptions.Builder, alluxio.grpc.RunPOptionsOrBuilder> optionsBuilder_;
    /**
     * <code>optional .alluxio.grpc.RunPOptions options = 2;</code>
     */
    public boolean hasOptions() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional .alluxio.grpc.RunPOptions options = 2;</code>
     */
    public alluxio.grpc.RunPOptions getOptions() {
      if (optionsBuilder_ == null) {
        return options_ == null ? alluxio.grpc.RunPOptions.getDefaultInstance() : options_;
      } else {
        return optionsBuilder_.getMessage();
      }
    }
    /**
     * <code>optional .alluxio.grpc.RunPOptions options = 2;</code>
     */
    public Builder setOptions(alluxio.grpc.RunPOptions value) {
      if (optionsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        options_ = value;
        onChanged();
      } else {
        optionsBuilder_.setMessage(value);
      }
      bitField0_ |= 0x00000002;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.RunPOptions options = 2;</code>
     */
    public Builder setOptions(
        alluxio.grpc.RunPOptions.Builder builderForValue) {
      if (optionsBuilder_ == null) {
        options_ = builderForValue.build();
        onChanged();
      } else {
        optionsBuilder_.setMessage(builderForValue.build());
      }
      bitField0_ |= 0x00000002;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.RunPOptions options = 2;</code>
     */
    public Builder mergeOptions(alluxio.grpc.RunPOptions value) {
      if (optionsBuilder_ == null) {
        if (((bitField0_ & 0x00000002) == 0x00000002) &&
            options_ != null &&
            options_ != alluxio.grpc.RunPOptions.getDefaultInstance()) {
          options_ =
            alluxio.grpc.RunPOptions.newBuilder(options_).mergeFrom(value).buildPartial();
        } else {
          options_ = value;
        }
        onChanged();
      } else {
        optionsBuilder_.mergeFrom(value);
      }
      bitField0_ |= 0x00000002;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.RunPOptions options = 2;</code>
     */
    public Builder clearOptions() {
      if (optionsBuilder_ == null) {
        options_ = null;
        onChanged();
      } else {
        optionsBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000002);
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.RunPOptions options = 2;</code>
     */
    public alluxio.grpc.RunPOptions.Builder getOptionsBuilder() {
      bitField0_ |= 0x00000002;
      onChanged();
      return getOptionsFieldBuilder().getBuilder();
    }
    /**
     * <code>optional .alluxio.grpc.RunPOptions options = 2;</code>
     */
    public alluxio.grpc.RunPOptionsOrBuilder getOptionsOrBuilder() {
      if (optionsBuilder_ != null) {
        return optionsBuilder_.getMessageOrBuilder();
      } else {
        return options_ == null ?
            alluxio.grpc.RunPOptions.getDefaultInstance() : options_;
      }
    }
    /**
     * <code>optional .alluxio.grpc.RunPOptions options = 2;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.RunPOptions, alluxio.grpc.RunPOptions.Builder, alluxio.grpc.RunPOptionsOrBuilder> 
        getOptionsFieldBuilder() {
      if (optionsBuilder_ == null) {
        optionsBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            alluxio.grpc.RunPOptions, alluxio.grpc.RunPOptions.Builder, alluxio.grpc.RunPOptionsOrBuilder>(
                getOptions(),
                getParentForChildren(),
                isClean());
        options_ = null;
      }
      return optionsBuilder_;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.RunPRequest)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.RunPRequest)
  private static final alluxio.grpc.RunPRequest DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.RunPRequest();
  }

  public static alluxio.grpc.RunPRequest getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<RunPRequest>
      PARSER = new com.google.protobuf.AbstractParser<RunPRequest>() {
    public RunPRequest parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new RunPRequest(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<RunPRequest> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<RunPRequest> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.RunPRequest getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}
