// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: sasl_server.proto

package alluxio.grpc;

public interface SaslMessageOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.SaslMessage)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional .alluxio.grpc.SaslMessageType messageType = 1;</code>
   */
  boolean hasMessageType();
  /**
   * <code>optional .alluxio.grpc.SaslMessageType messageType = 1;</code>
   */
  alluxio.grpc.SaslMessageType getMessageType();

  /**
   * <code>optional bytes message = 2;</code>
   */
  boolean hasMessage();
  /**
   * <code>optional bytes message = 2;</code>
   */
  com.google.protobuf.ByteString getMessage();

  /**
   * <code>optional string clientId = 3;</code>
   */
  boolean hasClientId();
  /**
   * <code>optional string clientId = 3;</code>
   */
  java.lang.String getClientId();
  /**
   * <code>optional string clientId = 3;</code>
   */
  com.google.protobuf.ByteString
      getClientIdBytes();

  /**
   * <code>optional string authenticationName = 4;</code>
   */
  boolean hasAuthenticationName();
  /**
   * <code>optional string authenticationName = 4;</code>
   */
  java.lang.String getAuthenticationName();
  /**
   * <code>optional string authenticationName = 4;</code>
   */
  com.google.protobuf.ByteString
      getAuthenticationNameBytes();
}
