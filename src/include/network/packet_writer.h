#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "network/postgres/postgres_protocol_utils.h"
#include "type/transient_value_peeker.h"

namespace terrier::network {

/**
 * Wrapper around an I/O layer WriteQueue to provide network protocol
 * helper methods.
 */
class PacketWriter {
 public:
  /**
   * Instantiates a new PacketWriter backed by the given WriteQueue
   */
  explicit PacketWriter(const common::ManagedPointer<WriteQueue> write_queue) : queue_(write_queue) {}

  ~PacketWriter() {
    // Make sure no packet is being written on destruction, otherwise we are
    // malformed write buffer
    TERRIER_ASSERT(curr_packet_len_ == nullptr, "packet length is not null");
  }

  /**
   * Checks whether the packet is null
   * @return whether packet is null
   */
  bool IsPacketEmpty() { return curr_packet_len_ == nullptr; }

  /**
   * Write out a single type
   * @param type to write to the queue
   */
  void WriteType(NetworkMessageType type) { queue_->BufferWriteRawValue(type); }

  /**
   * Write out a packet with a single type that is not related to Postgres SSL.
   * @param type Type of message to write out
   */
  void WriteSingleTypePacket(NetworkMessageType type) {
    // Make sure no active packet being constructed
    TERRIER_ASSERT(IsPacketEmpty(), "packet length is null");
    TERRIER_ASSERT(type != NetworkMessageType::PG_SSL_YES && type != NetworkMessageType::PG_SSL_NO,
                   "SSL types not allowed");
    BeginPacket(type).EndPacket();
  }

  /**
   * Begin writing a new packet. Caller can use other append methods to write
   * contents to the packet. An explicit call to end packet must be made to
   * make these writes valid.
   * @param type
   * @return self-reference for chaining
   */
  PacketWriter &BeginPacket(NetworkMessageType type) {
    // No active packet being constructed
    TERRIER_ASSERT(IsPacketEmpty(), "packet length is null");
    if (type != NetworkMessageType::NO_HEADER) WriteType(type);
    // Remember the size field since we will need to modify it as we go along.
    // It is important that our size field is contiguous and not broken between
    // two buffers.
    queue_->BufferWriteRawValue<int32_t>(0, false);
    WriteBuffer &tail = *(queue_->buffers_[queue_->buffers_.size() - 1]);
    curr_packet_len_ = reinterpret_cast<uint32_t *>(&tail.buf_[tail.size_ - sizeof(int32_t)]);
    return *this;
  }

  /**
   * Append raw bytes from specified memory location into the write queue.
   * There must be a packet active in the writer.
   * @param src memory location to write from
   * @param len number of bytes to write
   * @return self-reference for chaining
   */
  PacketWriter &AppendRaw(const void *src, size_t len) {
    TERRIER_ASSERT(!IsPacketEmpty(), "packet length is null");
    queue_->BufferWriteRaw(src, len);
    // Add the size field to the len of the packet. Be mindful of byte
    // ordering. We switch to network ordering only when the packet is finished
    *curr_packet_len_ += static_cast<uint32_t>(len);
    return *this;
  }

  /**
   * Append a value onto the write queue. There must be a packet active in the
   * writer. No byte order conversion is performed. It is up to the caller to
   * do so if needed.
   * @tparam T type of value to write
   * @param val value to write
   * @return self-reference for chaining
   */
  template <typename T>
  PacketWriter &AppendRawValue(T val) {
    return AppendRaw(&val, sizeof(T));
  }

  /**
   * Append a value of specified length onto the write queue. (1, 2, 4, or 8
   * bytes). It is assumed that these bytes need to be converted to network
   * byte ordering.
   * @tparam T type of value to read off. Has to be size 1, 2, 4, or 8.
   * @param val value to write
   * @return self-reference for chaining
   */
  template <typename T>
  PacketWriter &AppendValue(T val) {
    // We only want to allow for certain type sizes to be used
    // After the static assert, the compiler should be smart enough to throw
    // away the other cases and only leave the relevant return statement.
    static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8, "Invalid size for integer");

    switch (sizeof(T)) {
      case 1:
        return AppendRawValue(val);
      case 2:
        return AppendRawValue(_CAST(T, htobe16(_CAST(uint16_t, val))));
      case 4:
        return AppendRawValue(_CAST(T, htobe32(_CAST(uint32_t, val))));
      case 8:
        return AppendRawValue(_CAST(T, htobe64(_CAST(uint64_t, val))));
        // Will never be here due to compiler optimization
      default:
        throw NETWORK_PROCESS_EXCEPTION("invalid size for integer");
    }
  }

  /**
   * Append a string onto the write queue.
   * @param str the string to append
   * @param nul_terminate whether the nul terminaor should be written as well
   * @return self-reference for chaining
   */
  PacketWriter &AppendString(const std::string &str, bool nul_terminate = true) {
    return AppendRaw(str.data(), nul_terminate ? str.size() + 1 : str.size());
  }

  /**
   * Writes error responses to the client
   * @param error_status The error messages to send
   */
  void WriteErrorResponse(const std::vector<std::pair<NetworkMessageType, std::string>> &error_status) {
    BeginPacket(NetworkMessageType::PG_ERROR_RESPONSE);

    for (const auto &entry : error_status) AppendRawValue(entry.first).AppendString(entry.second);

    // Nul-terminate packet
    AppendRawValue<uchar>(0).EndPacket();
  }

  /**
   * A helper function to write a single error message without having to make a vector every time.
   * @param type
   * @param status
   */
  void WriteSingleErrorResponse(NetworkMessageType type, const std::string &status) {
    std::vector<std::pair<NetworkMessageType, std::string>> buf;
    buf.emplace_back(type, status);
    WriteErrorResponse(buf);
  }

  /**
   * Notify the client a readiness to receive a query
   * @param txn_status
   */
  void WriteReadyForQuery(NetworkTransactionStateType txn_status) {
    BeginPacket(NetworkMessageType::PG_READY_FOR_QUERY).AppendRawValue(txn_status).EndPacket();
  }

  /**
   * Writes response to startup message
   */
  void WriteStartupResponse() {
    BeginPacket(NetworkMessageType::PG_AUTHENTICATION_REQUEST).AppendValue<int32_t>(0).EndPacket();

    for (auto &entry : PG_PARAMETER_STATUS_MAP)
      BeginPacket(NetworkMessageType::PG_PARAMETER_STATUS)
          .AppendString(entry.first)
          .AppendString(entry.second)
          .EndPacket();
    WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  }

  /**
   * Writes the startup message, used by clients
   */
  void WriteStartupRequest(const std::unordered_map<std::string, std::string> &config, int16_t major_version = 3) {
    // Build header, assume minor version is always 0
    BeginPacket(NetworkMessageType::NO_HEADER).AppendValue<int16_t>(major_version).AppendValue<int16_t>(0);
    for (const auto &pair : config) AppendString(pair.first).AppendString(pair.second);
    AppendRawValue<uchar>(0);  // Startup message should have (byte+1) length
    EndPacket();
  }

  /**
   * End the packet. A packet write must be in progress and said write is not
   * well-formed until this method is called.
   */
  void EndPacket() {
    TERRIER_ASSERT(!IsPacketEmpty(), "packet length is null");
    // Switch to network byte ordering, add the 4 bytes of size field
    *curr_packet_len_ = htonl(*curr_packet_len_ + static_cast<uint32_t>(sizeof(uint32_t)));
    curr_packet_len_ = nullptr;
  }

 private:
  // We need to keep track of the size field of the current packet,
  // so we can update it as more bytes are written into this packet.
  uint32_t *curr_packet_len_ = nullptr;
  // Underlying WriteQueue backing this writer
  const common::ManagedPointer<WriteQueue> queue_;
};

}  // namespace terrier::network
