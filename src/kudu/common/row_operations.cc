// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/common/row_operations.h"

#include <cstring>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/faststring.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/safe_math.h"
#include "kudu/util/slice.h"

using std::string;
using std::vector;
using strings::Substitute;

DEFINE_int32(max_cell_size_bytes, 64 * 1024,
             "The maximum size of any individual cell in a table. Attempting to store "
             "string or binary columns with a size greater than this will result "
             "in errors.");
TAG_FLAG(max_cell_size_bytes, unsafe);

namespace kudu {

string DecodedRowOperation::ToString(const Schema& schema) const {
  if (!result.ok()) {
    return Substitute("row error: $0", result.ToString());
  }
  // A note on redaction: We redact row operations, since they contain sensitive
  // row data. Range partition operations are not redacted, since range
  // partitions are considered to be metadata.
  switch (type) {
    case RowOperationsPB::UNKNOWN:
      return "UNKNOWN";
    case RowOperationsPB::INSERT:
      return "INSERT " + schema.DebugRow(ConstContiguousRow(&schema, row_data));
    case RowOperationsPB::INSERT_IGNORE:
      return "INSERT IGNORE " + schema.DebugRow(ConstContiguousRow(&schema, row_data));
    case RowOperationsPB::UPSERT:
      return "UPSERT " + schema.DebugRow(ConstContiguousRow(&schema, row_data));
    case RowOperationsPB::UPSERT_IGNORE:
      return "UPSERT IGNORE " + schema.DebugRow(ConstContiguousRow(&schema, row_data));
    case RowOperationsPB::UPDATE:
    case RowOperationsPB::UPDATE_IGNORE:
    case RowOperationsPB::DELETE:
    case RowOperationsPB::DELETE_IGNORE:
      return Substitute("MUTATE $0 $1",
                        schema.DebugRowKey(ConstContiguousRow(&schema, row_data)),
                        changelist.ToString(schema));
    case RowOperationsPB::SPLIT_ROW:
      return Substitute("SPLIT_ROW $0", KUDU_DISABLE_REDACTION(split_row->ToString()));
    case RowOperationsPB::RANGE_LOWER_BOUND:
      return Substitute("RANGE_LOWER_BOUND $0", KUDU_DISABLE_REDACTION(split_row->ToString()));
    case RowOperationsPB::RANGE_UPPER_BOUND:
      return Substitute("RANGE_UPPER_BOUND $0", KUDU_DISABLE_REDACTION(split_row->ToString()));
    case RowOperationsPB::EXCLUSIVE_RANGE_LOWER_BOUND:
      return Substitute("EXCLUSIVE_RANGE_LOWER_BOUND $0",
                        KUDU_DISABLE_REDACTION(split_row->ToString()));
    case RowOperationsPB::INCLUSIVE_RANGE_UPPER_BOUND:
      return Substitute("INCLUSIVE_RANGE_UPPER_BOUND $0",
                        KUDU_DISABLE_REDACTION(split_row->ToString()));
  }
  return "UNKNOWN";
}

void DecodedRowOperation::SetFailureStatusOnce(Status s) {
  DCHECK(!s.ok());
  if (result.ok()) {
    result = std::move(s);
  }
}

RowOperationsPBEncoder::RowOperationsPBEncoder(RowOperationsPB* pb)
    : pb_(pb),
      prev_indirect_data_size_(string::npos),
      prev_rows_size_(string::npos) {
}

RowOperationsPBEncoder::~RowOperationsPBEncoder() {
}

size_t RowOperationsPBEncoder::Add(RowOperationsPB::Type op_type,
                                   const KuduPartialRow& partial_row) {
  const Schema* schema = partial_row.schema();

  // See wire_protocol.proto for a description of the format.
  string* dst = pb_->mutable_rows();
  prev_rows_size_ = dst->size();
  prev_indirect_data_size_ = pb_->mutable_indirect_data()->size();

  // Compute a bound on how much space we may need in the 'rows' field.
  // Then, resize it to this much space. This allows us to use simple
  // memcpy() calls to copy the data, rather than string->append(), which
  // reduces branches significantly in this fairly hot code path.
  // (std::string::append doesn't get inlined).
  // At the end of the function, we'll resize() the string back down to the
  // right size.
  size_t isset_bitmap_size;
  size_t non_null_bitmap_size;
  const size_t new_size_estimate = GetRowsFieldSizeEstimate(
      partial_row, &isset_bitmap_size, &non_null_bitmap_size);
  dst->resize(new_size_estimate);

  uint8_t* dst_ptr = reinterpret_cast<uint8_t*>(&(*dst)[prev_rows_size_]);
  *dst_ptr++ = static_cast<uint8_t>(op_type);
  memcpy(dst_ptr, partial_row.isset_bitmap_, isset_bitmap_size);
  dst_ptr += isset_bitmap_size;

  memcpy(dst_ptr,
         ContiguousRowHelper::non_null_bitmap_ptr(*schema, partial_row.row_data_),
         non_null_bitmap_size);
  dst_ptr += non_null_bitmap_size;

  size_t indirect_data_size_delta = 0;
  ContiguousRow row(schema, partial_row.row_data_);
  for (auto i = 0; i < schema->num_columns(); ++i) {
    if (!partial_row.IsColumnSet(i)) {
      continue;
    }

    const ColumnSchema& col = schema->column(i);
    if (col.is_nullable() && row.is_null(i)) {
      continue;
    }

    if (col.type_info()->physical_type() == BINARY) {
      const size_t indirect_offset = pb_->mutable_indirect_data()->size();
      const Slice* val = reinterpret_cast<const Slice*>(row.cell_ptr(i));
      indirect_data_size_delta += val->size();
      pb_->mutable_indirect_data()->append(
          reinterpret_cast<const char*>(val->data()), val->size());
      Slice to_append(
          reinterpret_cast<const uint8_t*>(indirect_offset), val->size());
      memcpy(dst_ptr, &to_append, sizeof(Slice));
      dst_ptr += sizeof(Slice);
    } else {
      memcpy(dst_ptr, row.cell_ptr(i), col.type_info()->size());
      dst_ptr += col.type_info()->size();
    }
  }

  const size_t rows_size_delta = reinterpret_cast<uintptr_t>(dst_ptr) -
      reinterpret_cast<uintptr_t>(&(*dst)[prev_rows_size_]);
  dst->resize(prev_rows_size_ + rows_size_delta);

  return rows_size_delta + indirect_data_size_delta;
}

void RowOperationsPBEncoder::RemoveLast() {
  DCHECK_NE(string::npos, prev_indirect_data_size_);
  DCHECK_NE(string::npos, prev_rows_size_);
  pb_->mutable_indirect_data()->resize(prev_indirect_data_size_);
  pb_->mutable_rows()->resize(prev_rows_size_);
  prev_indirect_data_size_ = string::npos;
  prev_rows_size_ = string::npos;
}

size_t RowOperationsPBEncoder::GetRowsFieldSizeEstimate(
    const KuduPartialRow& partial_row,
    size_t* isset_bitmap_size,
    size_t* isnon_null_bitmap_size) const {
  const Schema* schema = partial_row.schema();

  // See wire_protocol.proto for a description of the format.
  const string* dst = pb_->mutable_rows();

  auto isset_size = BitmapSize(schema->num_columns());
  auto isnull_size = ContiguousRowHelper::non_null_bitmap_size(*schema);
  constexpr auto type_size = 1; // type uses one byte
  auto max_size = type_size + schema->byte_size() + isset_size + isnull_size;

  *isset_bitmap_size = isset_size;
  *isnon_null_bitmap_size = isnull_size;
  return dst->size() + max_size;
}

// ------------------------------------------------------------
// Decoder
// ------------------------------------------------------------

RowOperationsPBDecoder::RowOperationsPBDecoder(const RowOperationsPB* pb,
                                               const Schema* client_schema,
                                               const Schema* tablet_schema,
                                               Arena* dst_arena)
  : pb_(pb),
    client_schema_(client_schema),
    tablet_schema_(tablet_schema),
    dst_arena_(dst_arena),
    bm_size_(BitmapSize(client_schema_->num_columns())),
    tablet_row_size_(ContiguousRowHelper::row_size(*tablet_schema_)),
    src_(pb->rows().data(), pb->rows().size()) {
}

RowOperationsPBDecoder::~RowOperationsPBDecoder() {
}

Status RowOperationsPBDecoder::ReadOpType(RowOperationsPB::Type* type) {
  if (PREDICT_FALSE(src_.empty())) {
    return Status::Corruption("Cannot find operation type");
  }
  if (PREDICT_FALSE(!RowOperationsPB_Type_IsValid(src_[0]))) {
    return Status::NotSupported(Substitute("Unknown operation type: $0", src_[0]));
  }
  *type = static_cast<RowOperationsPB::Type>(src_[0]);
  src_.remove_prefix(1);
  return Status::OK();
}

Status RowOperationsPBDecoder::ReadIssetBitmap(const uint8_t** bitmap) {
  if (PREDICT_FALSE(src_.size() < bm_size_)) {
    *bitmap = nullptr;
    return Status::Corruption("Cannot find isset bitmap");
  }
  *bitmap = src_.data();
  src_.remove_prefix(bm_size_);
  return Status::OK();
}

Status RowOperationsPBDecoder::ReadNonNullBitmap(const uint8_t** bitmap) {
  if (PREDICT_FALSE(src_.size() < bm_size_)) {
    *bitmap = nullptr;
    return Status::Corruption("Cannot find null bitmap");
  }
  *bitmap = src_.data();
  src_.remove_prefix(bm_size_);
  return Status::OK();
}

Status RowOperationsPBDecoder::GetColumnSlice(const ColumnSchema& col,
                                              Slice* slice,
                                              Status* row_status) {
  size_t size = col.type_info()->size();
  if (PREDICT_FALSE(src_.size() < size)) {
    return Status::Corruption("Not enough data for column", col.ToString());
  }
  // Find the data
  if (col.type_info()->physical_type() == BINARY) {
    // The Slice in the protobuf has a pointer relative to the indirect data,
    // not a real pointer. Need to fix that.
    auto ptr_slice = reinterpret_cast<const Slice*>(src_.data());
    auto offset_in_indirect = reinterpret_cast<uintptr_t>(ptr_slice->data());
    bool overflowed = false;
    size_t max_offset = AddWithOverflowCheck(offset_in_indirect, ptr_slice->size(), &overflowed);
    if (PREDICT_FALSE(overflowed || max_offset > pb_->indirect_data().size())) {
      return Status::Corruption("Bad indirect slice");
    }

    // Check that no individual cell is larger than the specified max.
    if (PREDICT_FALSE(row_status && ptr_slice->size() > FLAGS_max_cell_size_bytes)) {
      *row_status = Status::InvalidArgument(Substitute(
          "value too large for column '$0' ($1 bytes, maximum is $2 bytes)",
          col.name(), ptr_slice->size(), FLAGS_max_cell_size_bytes));
      // After one row's column size has been found to exceed the limit and has been recorded
      // in 'row_status', we will consider it OK and continue to consume data in order to properly
      // validate subsequent columns and rows.
    }
    *slice = Slice(&pb_->indirect_data()[offset_in_indirect], ptr_slice->size());
  } else {
    *slice = Slice(src_.data(), size);
  }
  src_.remove_prefix(size);
  return Status::OK();
}

Status RowOperationsPBDecoder::ReadColumn(const ColumnSchema& col,
                                          uint8_t* dst,
                                          Status* row_status) {
  Slice slice;
  RETURN_NOT_OK(GetColumnSlice(col, &slice, row_status));
  if (col.type_info()->physical_type() == BINARY) {
    memcpy(dst, &slice, col.type_info()->size());
  } else {
    slice.relocate(dst);
  }
  return Status::OK();
}

Status RowOperationsPBDecoder::ReadColumnAndDiscard(const ColumnSchema& col) {
  uint8_t scratch[kLargestTypeSize];
  return ReadColumn(col, scratch, nullptr);
}

bool RowOperationsPBDecoder::HasNext() const {
  return !src_.empty();
}

namespace {

void SetupPrototypeRow(const Schema& schema,
                       ContiguousRow* row) {
  for (int i = 0; i < schema.num_columns(); i++) {
    const ColumnSchema& col = schema.column(i);
    if (col.has_write_default()) {
      if (col.is_nullable()) {
        row->set_null(i, false);
      }
      memcpy(row->mutable_cell_ptr(i), col.write_default_value(), col.type_info()->size());
    } else if (col.is_nullable()) {
      row->set_null(i, true);
    } else {
      // No default and not nullable. Therefore this column is required,
      // and we'll ensure that it gets during the projection step.
    }
  }
}
} // anonymous namespace

// Projector implementation which handles mapping the client column indexes
// to server-side column indexes, ensuring that all of the columns exist,
// and that every required (non-null, non-default) column in the server
// schema is also present in the client.
class ClientServerMapping {
 public:
  ClientServerMapping(const Schema* client_schema,
                      const Schema* tablet_schema)
    : client_schema_(client_schema),
      tablet_schema_(tablet_schema),
      saw_tablet_col_(tablet_schema->num_columns(), false) {
  }

  Status ProjectBaseColumn(size_t client_col_idx, size_t tablet_col_idx) {
    // We should get this called exactly once for every input column,
    // since the input columns must be a strict subset of the tablet columns.
    DCHECK_EQ(client_to_tablet_.size(), client_col_idx);
    DCHECK_LT(tablet_col_idx, saw_tablet_col_.size());
    client_to_tablet_.push_back(tablet_col_idx);
    saw_tablet_col_[tablet_col_idx] = true;
    return Status::OK();
  }

  Status ProjectDefaultColumn(size_t client_col_idx) {
    // Even if the client provides a default (which it shouldn't), we don't
    // want to accept writes with an extra column.
    return ProjectExtraColumn(client_col_idx);
  }

  Status ProjectExtraColumn(size_t client_col_idx) {
    return Status::InvalidArgument(
      Substitute("Client provided column $0 not present in tablet",
                 client_schema_->column(client_col_idx).ToString()));
  }

  // Translate from a client schema index to the tablet schema index
  size_t client_to_tablet_idx(size_t client_idx) const {
    DCHECK_LT(client_idx, client_to_tablet_.size());
    return client_to_tablet_[client_idx];
  }

  size_t num_mapped() const {
    return client_to_tablet_.size();
  }

  // Ensure that any required (non-null, non-defaulted) columns from the
  // server side schema are found in the client-side schema. If not,
  // returns an InvalidArgument.
  Status CheckAllRequiredColumnsPresent() {
    for (size_t tablet_col_idx = 0;
         tablet_col_idx < tablet_schema_->num_columns();
         tablet_col_idx++) {
      const ColumnSchema& col = tablet_schema_->column(tablet_col_idx);
      if (!col.has_write_default() &&
          !col.is_nullable()) {
        // All clients must pass this column.
        if (!saw_tablet_col_[tablet_col_idx]) {
          return Status::InvalidArgument(
            "Client missing required column", col.ToString());
        }
      }
    }
    return Status::OK();
  }

 private:
  const Schema* const client_schema_;
  const Schema* const tablet_schema_;
  vector<size_t> client_to_tablet_;
  vector<bool> saw_tablet_col_;
  DISALLOW_COPY_AND_ASSIGN(ClientServerMapping);
};

size_t RowOperationsPBDecoder::GetTabletColIdx(const ClientServerMapping& mapping,
                                               size_t client_col_idx) {
  if (client_schema_ != tablet_schema_) {
    return mapping.client_to_tablet_idx(client_col_idx);
  }
  return client_col_idx;
}

Status RowOperationsPBDecoder::DecodeInsertOrUpsert(const uint8_t* prototype_row_storage,
                                                    const ClientServerMapping& mapping,
                                                    DecodedRowOperation* op,
                                                    int64_t* auto_incrementing_counter) {
  const uint8_t* client_isset_map = nullptr;
  const uint8_t* client_non_null_map = nullptr;

  // Read the null and isset bitmaps for the client-provided row.
  RETURN_NOT_OK(ReadIssetBitmap(&client_isset_map));
  if (client_schema_->has_nullables()) {
    RETURN_NOT_OK(ReadNonNullBitmap(&client_non_null_map));
  }

  // Allocate a row with the tablet's layout.
  auto* tablet_row_storage = reinterpret_cast<uint8_t*>(
      dst_arena_->AllocateBytesAligned(tablet_row_size_, 8));
  auto* tablet_isset_bitmap = reinterpret_cast<uint8_t*>(
      dst_arena_->AllocateBytes(BitmapSize(tablet_schema_->num_columns())));
  if (PREDICT_FALSE(!tablet_row_storage || !tablet_isset_bitmap)) {
    return Status::RuntimeError("Out of memory");
  }
  // Initialize the bitmap since some columns might be lost in the client schema,
  // in which case the original value of the lost columns might be set to default
  // value by upsert request.
  memset(tablet_isset_bitmap, 0, BitmapSize(tablet_schema_->num_columns()));

  // Initialize the new row from the 'prototype' row which has been set
  // with all of the server-side default values. This copy may be entirely
  // overwritten in the case that all columns are specified, but this is
  // still likely faster (and simpler) than looping through all the server-side
  // columns to initialize defaults where non-set on every row.
  memcpy(tablet_row_storage, prototype_row_storage, tablet_row_size_);
  ContiguousRow tablet_row(tablet_schema_, tablet_row_storage);

  // Now handle each of the columns passed by the user, replacing the defaults
  // from the prototype.
  const auto auto_incrementing_col_idx = tablet_schema_->auto_incrementing_col_idx();
  for (size_t client_col_idx = 0;
       client_col_idx < client_schema_->num_columns();
       client_col_idx++) {
    // Look up the corresponding column from the tablet. We use the server-side
    // ColumnSchema object since it has the most up-to-date default, nullability,
    // etc.
    size_t tablet_col_idx = GetTabletColIdx(mapping, client_col_idx);
    DCHECK_NE(tablet_col_idx, Schema::kColumnNotFound);
    const ColumnSchema& col = tablet_schema_->column(tablet_col_idx);

    bool isset = BitmapTest(client_isset_map, client_col_idx);
    BitmapChange(tablet_isset_bitmap, tablet_col_idx, isset);
    if (isset) {
      // If the client provided a value for this column, copy it.
      // Copy null-ness, if the server side column is nullable.
      const bool client_set_to_null = client_schema_->has_nullables() &&
          BitmapTest(client_non_null_map, client_col_idx);
      if (col.is_nullable()) {
        tablet_row.set_null(tablet_col_idx, client_set_to_null);
      }
      if (!client_set_to_null) {
        // Check if the non-null value is present for auto-incrementing column. For UPSERT or
        // UPSERT_IGNORE operations we allow the user to specify the auto-incrementing value.
        if (tablet_col_idx != auto_incrementing_col_idx) {
          // Copy the non-null value.
          Status row_status;
          RETURN_NOT_OK(ReadColumn(
              col, tablet_row.mutable_cell_ptr(tablet_col_idx), &row_status));
          if (PREDICT_FALSE(!row_status.ok())) {
            op->SetFailureStatusOnce(row_status);
          }
        } else {
          if (op->type == RowOperationsPB_Type_INSERT ||
              op->type == RowOperationsPB_Type_INSERT_IGNORE) {
            // auto-incrementing column values not to be set for INSERT/INSERT_IGNORE operations.
            static const Status kErrFieldIncorrectlySet = Status::InvalidArgument(
                "auto-incrementing column should not be set for INSERT/INSERT_IGNORE operations");
            op->SetFailureStatusOnce(kErrFieldIncorrectlySet);
            RETURN_NOT_OK(ReadColumnAndDiscard(col));
            return kErrFieldIncorrectlySet;
          }
          // Fetch the auto-incrementing counter from the request.
          Status row_status;
          int64_t counter = 0;
          RETURN_NOT_OK(ReadColumn(
              col, reinterpret_cast<uint8_t*>(&counter), &row_status));
          if (PREDICT_FALSE(!row_status.ok())) {
            op->SetFailureStatusOnce(row_status);
          } else {
            // Make sure it is positive.
            if (counter < 0) {
              static const Status kErrorValue = Status::InvalidArgument(
                  "auto-incrementing column value must be greater than zero");
              op->SetFailureStatusOnce(kErrorValue);
              return kErrorValue;
            }
            // Check if the provided counter value is less than what is in memory
            // and update the counter in memory.
            if (counter > *auto_incrementing_counter) {
              *auto_incrementing_counter = counter;
            }
            memcpy(tablet_row.mutable_cell_ptr(tablet_col_idx), &counter, 8);
          }
        }
      } else if (PREDICT_FALSE(!col.is_nullable())) {
        op->SetFailureStatusOnce(Status::InvalidArgument(
            "NULL values not allowed for non-nullable column", col.ToString()));
        RETURN_NOT_OK(ReadColumnAndDiscard(col));
      }
    } else {
      // If the client didn't provide a value, check if it's an auto-incrementing
      // field. If so, populate the field as appropriate.
      if (tablet_col_idx == auto_incrementing_col_idx) {
        if (op->type == RowOperationsPB_Type_UPSERT ||
            op->type == RowOperationsPB_Type_UPSERT_IGNORE) {
          static const Status kErrMaxValue = Status::InvalidArgument("auto-incrementing column "
                                                                     "should be set for "
                                                                     "UPSERT/UPSERT_IGNORE "
                                                                     "operations");
          op->SetFailureStatusOnce(kErrMaxValue);
          return kErrMaxValue;
        }
        if (*DCHECK_NOTNULL(auto_incrementing_counter) == INT64_MAX) {
          static const Status kErrMaxValue = Status::IllegalState("max auto-incrementing column "
                                                                   "value reached");
          op->SetFailureStatusOnce(kErrMaxValue);
          return kErrMaxValue;
        }
        if (*DCHECK_NOTNULL(auto_incrementing_counter) < 0) {
          static const Status kErrValue = Status::IllegalState("invalid auto-incrementing "
                                                               "column value");
          op->SetFailureStatusOnce(kErrValue);
          return kErrValue;
        }
        // We increment the auto incrementing counter at this point regardless of future failures
        // in the op for simplicity. The auto-incrementing column key space is large enough to
        // not run of values for any realistic workloads.
        (*auto_incrementing_counter)++;
        memcpy(tablet_row.mutable_cell_ptr(tablet_col_idx), auto_incrementing_counter, 8);
        BitmapChange(tablet_isset_bitmap, client_col_idx, true);
      } else if (PREDICT_FALSE(!(col.is_nullable() || col.has_write_default()))) {
        // Otherwise, the column must either be nullable or have a default (which
        // was already set in the prototype row).
        op->SetFailureStatusOnce(Status::InvalidArgument("No value provided for required column",
                                                         col.ToString()));
      }
    }
  }

  op->row_data = tablet_row_storage;
  op->isset_bitmap = tablet_isset_bitmap;
  return Status::OK();
}

Status RowOperationsPBDecoder::DecodeUpdateOrDelete(const ClientServerMapping& mapping,
                                                    DecodedRowOperation* op) {
  const uint8_t* client_isset_map = nullptr;
  const uint8_t* client_non_null_map = nullptr;

  // Read the null and isset bitmaps for the client-provided row.
  RETURN_NOT_OK(ReadIssetBitmap(&client_isset_map));
  if (client_schema_->has_nullables()) {
    RETURN_NOT_OK(ReadNonNullBitmap(&client_non_null_map));
  }

  // Allocate space for the row key.
  auto* rowkey_storage = reinterpret_cast<uint8_t*>(
      dst_arena_->AllocateBytesAligned(tablet_schema_->key_byte_size(), 8));
  if (PREDICT_FALSE(!rowkey_storage)) {
    return Status::RuntimeError("Out of memory");
  }

  // We're passing the full schema instead of the key schema here.
  // That's OK because the keys come at the bottom. We lose some bounds
  // checking in debug builds, but it avoids an extra copy of the key schema.
  ContiguousRow rowkey(tablet_schema_, rowkey_storage);

  // First process the key columns.
  size_t client_col_idx = 0;
  for (; client_col_idx < client_schema_->num_key_columns(); client_col_idx++) {
    // Look up the corresponding column from the tablet. We use the server-side
    // ColumnSchema object since it has the most up-to-date default, nullability,
    // etc.
    if (client_schema_ != tablet_schema_) {
      DCHECK_EQ(mapping.client_to_tablet_idx(client_col_idx),
                client_col_idx) << "key columns should match";
    }
    size_t tablet_col_idx = client_col_idx;

    const ColumnSchema& col = tablet_schema_->column(tablet_col_idx);
    if (PREDICT_FALSE(!BitmapTest(client_isset_map, client_col_idx))) {
      op->SetFailureStatusOnce(Status::InvalidArgument("No value provided for key column",
                                                       col.ToString()));
      continue;
    }

    bool client_set_to_null = client_schema_->has_nullables() &&
        BitmapTest(client_non_null_map, client_col_idx);
    if (PREDICT_FALSE(client_set_to_null)) {
      op->SetFailureStatusOnce(Status::InvalidArgument("NULL values not allowed for key column",
                                                       col.ToString()));
      RETURN_NOT_OK(ReadColumnAndDiscard(col));
      continue;
    }

    RETURN_NOT_OK(ReadColumn(col, rowkey.mutable_cell_ptr(tablet_col_idx), nullptr));
  }
  op->row_data = rowkey_storage;

  // Now we process the rest of the columns:
  // For UPDATE, we expect at least one other column to be set, indicating the
  // update to perform.
  // For DELETE, we expect no other columns to be set (and we verify that).
  Status row_status;
  if (op->type == RowOperationsPB::UPDATE || op->type == RowOperationsPB::UPDATE_IGNORE) {
    faststring buf;
    RowChangeListEncoder rcl_encoder(&buf);

    // Now process the rest of columns as updates.
    DCHECK_EQ(client_schema_->num_key_columns(), client_col_idx);
    for (; client_col_idx < client_schema_->num_columns(); client_col_idx++) {
      size_t tablet_col_idx = GetTabletColIdx(mapping, client_col_idx);
      const ColumnSchema& col = tablet_schema_->column(tablet_col_idx);

      if (BitmapTest(client_isset_map, client_col_idx)) {
        bool client_set_to_null = client_schema_->has_nullables() &&
          BitmapTest(client_non_null_map, client_col_idx);

        if (col.is_immutable()) {
          if (op->type == RowOperationsPB::UPDATE) {
            op->SetFailureStatusOnce(
                Status::Immutable("UPDATE not allowed for immutable column", col.ToString()));
          } else {
            DCHECK_EQ(RowOperationsPB::UPDATE_IGNORE, op->type);
            op->error_ignored = true;
          }
          if (!client_set_to_null) {
            RETURN_NOT_OK(ReadColumnAndDiscard(col));
          }
          // Use 'continue' not 'break' to consume the rest row data.
          continue;
        }

        uint8_t scratch[kLargestTypeSize];
        uint8_t* val_to_add = nullptr;
        if (!client_set_to_null) {
          RETURN_NOT_OK(ReadColumn(col, scratch, &row_status));
          if (PREDICT_FALSE(!row_status.ok())) {
            op->SetFailureStatusOnce(row_status);
          }
          val_to_add = scratch;
        } else if (PREDICT_FALSE(!col.is_nullable())) {
          op->SetFailureStatusOnce(Status::InvalidArgument(
              "NULL value not allowed for non-nullable column", col.ToString()));
          RETURN_NOT_OK(ReadColumnAndDiscard(col));
          continue;
        }

        rcl_encoder.AddColumnUpdate(col, tablet_schema_->column_id(tablet_col_idx), val_to_add);
      }
    }

    if (PREDICT_FALSE(buf.size() == 0)) {
      // No actual column updates specified!
      op->SetFailureStatusOnce(Status::InvalidArgument("No fields updated, key is",
                                                       tablet_schema_->DebugRowKey(rowkey)));
    }

    if (PREDICT_TRUE(op->result.ok())) {
      // Copy the row-changelist to the arena.
      auto* rcl_in_arena = reinterpret_cast<uint8_t*>(
          dst_arena_->AllocateBytesAligned(buf.size(), 8));
      if (PREDICT_FALSE(rcl_in_arena == nullptr)) {
        return Status::RuntimeError("Out of memory allocating RCL");
      }
      memcpy(rcl_in_arena, buf.data(), buf.size());
      op->changelist = RowChangeList(Slice(rcl_in_arena, buf.size()));
    }
  } else if (op->type == RowOperationsPB::DELETE || op->type == RowOperationsPB::DELETE_IGNORE) {
    // Ensure that no other columns are set.
    for (; client_col_idx < client_schema_->num_columns(); client_col_idx++) {
      if (PREDICT_FALSE(BitmapTest(client_isset_map, client_col_idx))) {
        size_t tablet_col_idx = GetTabletColIdx(mapping, client_col_idx);
        const ColumnSchema& col = tablet_schema_->column(tablet_col_idx);
        op->SetFailureStatusOnce(Status::InvalidArgument(
            "DELETE should not have a value for column", col.ToString()));

        bool client_set_to_null = client_schema_->has_nullables() &&
            BitmapTest(client_non_null_map, client_col_idx);
        if (!client_set_to_null || !col.is_nullable()) {
          RETURN_NOT_OK(ReadColumnAndDiscard(col));
        }
      }
    }
    if (PREDICT_TRUE(op->result.ok())) {
      op->changelist = RowChangeList::CreateDelete();
    }
  } else {
    LOG(FATAL) << "Should only call this method with UPDATE or DELETE";
  }

  return Status::OK();
}

Status RowOperationsPBDecoder::DecodeSplitRow(const ClientServerMapping& mapping,
                                              DecodedRowOperation* op) {
  op->split_row = std::make_shared<KuduPartialRow>(tablet_schema_);

  // Read the null and isset bitmaps for the client-provided row.
  const uint8_t* client_isset_map;
  RETURN_NOT_OK(ReadIssetBitmap(&client_isset_map));
  if (client_schema_->has_nullables()) {
    const uint8_t* client_non_null_map;
    RETURN_NOT_OK(ReadNonNullBitmap(&client_non_null_map));
  }

  // Now handle each of the columns passed by the user.
  for (size_t client_col_idx = 0;
       client_col_idx < client_schema_->num_columns();
       client_col_idx++) {
    // Look up the corresponding column from the tablet. We use the server-side
    // ColumnSchema object since it has the most up-to-date default, nullability,
    // etc.
    size_t tablet_col_idx = GetTabletColIdx(mapping, client_col_idx);
    const ColumnSchema& col = tablet_schema_->column(tablet_col_idx);

    if (BitmapTest(client_isset_map, client_col_idx)) {
      // If the client provided a value for this column, copy it.
      Slice column_slice;
      RETURN_NOT_OK(GetColumnSlice(col, &column_slice, nullptr));
      const uint8_t* data =  (col.type_info()->physical_type() == BINARY)
          ? reinterpret_cast<const uint8_t*>(&column_slice)
          : column_slice.data();
      RETURN_NOT_OK(op->split_row->Set(static_cast<int32_t>(tablet_col_idx), data));
    }
  }
  return Status::OK();
}

template <DecoderMode mode>
Status RowOperationsPBDecoder::DecodeOperations(vector<DecodedRowOperation>* ops,
                                                int64_t* auto_incrementing_counter) {
  // TODO(todd): there's a bug here, in that if a client passes some column in
  // its schema that has been deleted on the server, it will fail even if the
  // client never actually specified any values for it.  For example, a DBA
  // might do a thorough audit that no one is using some column anymore, and
  // then drop the column, expecting it to be compatible, but all writes would
  // start failing until clients refreshed their schema.
  // See DISABLED_TestProjectUpdatesSubsetOfColumns
  CHECK_EQ(client_schema_->has_column_ids(), client_schema_ == tablet_schema_);
  DCHECK(tablet_schema_->has_column_ids());
  ClientServerMapping mapping(client_schema_, tablet_schema_);
  if (client_schema_ != tablet_schema_) {
    RETURN_NOT_OK(client_schema_->GetProjectionMapping(*tablet_schema_, &mapping));
    DCHECK_EQ(mapping.num_mapped(), client_schema_->num_columns());
    RETURN_NOT_OK(mapping.CheckAllRequiredColumnsPresent());
  }

  // Make a "prototype row" which has all the defaults filled in. We can copy
  // this to create a starting point for each row as we decode it, with
  // all the defaults in place without having to loop.
  uint8_t prototype_row_storage[tablet_row_size_];
  ContiguousRow prototype_row(tablet_schema_, prototype_row_storage);
  SetupPrototypeRow(*tablet_schema_, &prototype_row);
  int64_t counter = -1;
  auto_incrementing_counter = auto_incrementing_counter ? auto_incrementing_counter : &counter;

  while (HasNext()) {
    RowOperationsPB::Type type = RowOperationsPB::UNKNOWN;
    RETURN_NOT_OK(ReadOpType(&type));

    DecodedRowOperation op;
    op.type = type;
    RETURN_NOT_OK(DecodeOp<mode>(type, prototype_row_storage, mapping, &op,
                                 auto_incrementing_counter));
    ops->emplace_back(std::move(op));
  }

  return Status::OK();
}

template<>
Status RowOperationsPBDecoder::DecodeOp<DecoderMode::WRITE_OPS>(
    RowOperationsPB::Type type, const uint8_t* prototype_row_storage,
    const ClientServerMapping& mapping, DecodedRowOperation* op,
    int64_t* auto_incrementing_counter) {
  switch (type) {
    case RowOperationsPB::UPSERT:
    case RowOperationsPB::UPSERT_IGNORE:
    case RowOperationsPB::INSERT:
    case RowOperationsPB::INSERT_IGNORE:
      return DecodeInsertOrUpsert(prototype_row_storage, mapping, op,
                                  auto_incrementing_counter);
    case RowOperationsPB::UPDATE:
    case RowOperationsPB::UPDATE_IGNORE:
    case RowOperationsPB::DELETE:
    case RowOperationsPB::DELETE_IGNORE:
      return DecodeUpdateOrDelete(mapping, op);
    default:
      break;
  }
  return Status::InvalidArgument(Substitute("Invalid write operation type $0",
                                            RowOperationsPB_Type_Name(type)));
}

template<>
Status RowOperationsPBDecoder::DecodeOp<DecoderMode::SPLIT_ROWS>(
    RowOperationsPB::Type type, const uint8_t* /*prototype_row_storage*/,
    const ClientServerMapping& mapping, DecodedRowOperation* op,
    int64_t* /*auto_incrementing_counter*/) {
  switch (type) {
    case RowOperationsPB::SPLIT_ROW:
    case RowOperationsPB::RANGE_LOWER_BOUND:
    case RowOperationsPB::RANGE_UPPER_BOUND:
    case RowOperationsPB::EXCLUSIVE_RANGE_LOWER_BOUND:
    case RowOperationsPB::INCLUSIVE_RANGE_UPPER_BOUND:
      return DecodeSplitRow(mapping, op);
    default:
      break;
  }
  return Status::InvalidArgument(Substitute("Invalid split row type $0",
                                            RowOperationsPB_Type_Name(type)));
}

template
Status RowOperationsPBDecoder::DecodeOperations<DecoderMode::SPLIT_ROWS>(
    vector<DecodedRowOperation>* ops, int64_t* auto_incrementing_counter);

template
Status RowOperationsPBDecoder::DecodeOperations<DecoderMode::WRITE_OPS>(
    vector<DecodedRowOperation>* ops, int64_t* auto_incrementing_counter);

} // namespace kudu
