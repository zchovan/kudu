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

#include "kudu/client/schema.h"

#include <memory>
#include <ostream>
#include <unordered_map>
#include <utility>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/client/schema-internal.h"
#include "kudu/client/value-internal.h"
#include "kudu/client/value.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/char_util.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/decimal_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/string_case.h"

DEFINE_bool(show_attributes, false,
            "Whether to show column attributes, including column encoding type, "
            "compression type, and default read/write value.");

MAKE_ENUM_LIMITS(kudu::client::KuduColumnStorageAttributes::EncodingType,
                 kudu::client::KuduColumnStorageAttributes::AUTO_ENCODING,
                 kudu::client::KuduColumnStorageAttributes::RLE);

MAKE_ENUM_LIMITS(kudu::client::KuduColumnStorageAttributes::CompressionType,
                 kudu::client::KuduColumnStorageAttributes::DEFAULT_COMPRESSION,
                 kudu::client::KuduColumnStorageAttributes::ZLIB);

MAKE_ENUM_LIMITS(kudu::client::KuduColumnSchema::DataType,
                 kudu::client::KuduColumnSchema::INT8,
                 kudu::client::KuduColumnSchema::BOOL);

using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace client {

kudu::EncodingType ToInternalEncodingType(KuduColumnStorageAttributes::EncodingType type) {
  switch (type) {
    case KuduColumnStorageAttributes::AUTO_ENCODING: return kudu::AUTO_ENCODING;
    case KuduColumnStorageAttributes::PLAIN_ENCODING: return kudu::PLAIN_ENCODING;
    case KuduColumnStorageAttributes::PREFIX_ENCODING: return kudu::PREFIX_ENCODING;
    case KuduColumnStorageAttributes::DICT_ENCODING: return kudu::DICT_ENCODING;
    case KuduColumnStorageAttributes::GROUP_VARINT: return kudu::GROUP_VARINT;
    case KuduColumnStorageAttributes::RLE: return kudu::RLE;
    case KuduColumnStorageAttributes::BIT_SHUFFLE: return kudu::BIT_SHUFFLE;
    default: LOG(FATAL) << "Unexpected encoding type: " << type;
  }
}

KuduColumnStorageAttributes::EncodingType FromInternalEncodingType(kudu::EncodingType type) {
  switch (type) {
    case kudu::AUTO_ENCODING: return KuduColumnStorageAttributes::AUTO_ENCODING;
    case kudu::PLAIN_ENCODING: return KuduColumnStorageAttributes::PLAIN_ENCODING;
    case kudu::PREFIX_ENCODING: return KuduColumnStorageAttributes::PREFIX_ENCODING;
    case kudu::DICT_ENCODING: return KuduColumnStorageAttributes::DICT_ENCODING;
    case kudu::GROUP_VARINT: return KuduColumnStorageAttributes::GROUP_VARINT;
    case kudu::RLE: return KuduColumnStorageAttributes::RLE;
    case kudu::BIT_SHUFFLE: return KuduColumnStorageAttributes::BIT_SHUFFLE;
    default: LOG(FATAL) << "Unexpected internal encoding type: " << type;
  }
}

kudu::CompressionType ToInternalCompressionType(KuduColumnStorageAttributes::CompressionType type) {
  switch (type) {
    case KuduColumnStorageAttributes::DEFAULT_COMPRESSION: return kudu::DEFAULT_COMPRESSION;
    case KuduColumnStorageAttributes::NO_COMPRESSION: return kudu::NO_COMPRESSION;
    case KuduColumnStorageAttributes::SNAPPY: return kudu::SNAPPY;
    case KuduColumnStorageAttributes::LZ4: return kudu::LZ4;
    case KuduColumnStorageAttributes::ZLIB: return kudu::ZLIB;
    default: LOG(FATAL) << "Unexpected compression type" << type;
  }
}

KuduColumnStorageAttributes::CompressionType FromInternalCompressionType(
    kudu::CompressionType type) {
  switch (type) {
    case kudu::DEFAULT_COMPRESSION: return KuduColumnStorageAttributes::DEFAULT_COMPRESSION;
    case kudu::NO_COMPRESSION: return KuduColumnStorageAttributes::NO_COMPRESSION;
    case kudu::SNAPPY: return KuduColumnStorageAttributes::SNAPPY;
    case kudu::LZ4: return KuduColumnStorageAttributes::LZ4;
    case kudu::ZLIB: return KuduColumnStorageAttributes::ZLIB;
    default: LOG(FATAL) << "Unexpected internal compression type: " << type;
  }
}

kudu::DataType ToInternalDataType(KuduColumnSchema::DataType type,
                                  const KuduColumnTypeAttributes& attributes) {
  switch (type) {
    case KuduColumnSchema::INT8: return kudu::INT8;
    case KuduColumnSchema::INT16: return kudu::INT16;
    case KuduColumnSchema::INT32: return kudu::INT32;
    case KuduColumnSchema::INT64: return kudu::INT64;
    case KuduColumnSchema::UNIXTIME_MICROS: return kudu::UNIXTIME_MICROS;
    case KuduColumnSchema::DATE: return kudu::DATE;
    case KuduColumnSchema::FLOAT: return kudu::FLOAT;
    case KuduColumnSchema::DOUBLE: return kudu::DOUBLE;
    case KuduColumnSchema::VARCHAR: return kudu::VARCHAR;
    case KuduColumnSchema::STRING: return kudu::STRING;
    case KuduColumnSchema::BINARY: return kudu::BINARY;
    case KuduColumnSchema::BOOL: return kudu::BOOL;
    case KuduColumnSchema::DECIMAL:
      if (attributes.precision() <= kMaxDecimal32Precision) {
        return kudu::DECIMAL32;
      } else if (attributes.precision() <= kMaxDecimal64Precision) {
        return kudu::DECIMAL64;
      } else if (attributes.precision() <= kMaxDecimal128Precision) {
        return kudu::DECIMAL128;
      } else {
        LOG(FATAL) << "Unsupported decimal type precision: " << attributes.precision();
      }
    default: LOG(FATAL) << "Unexpected data type: " << type;
  }
}

KuduColumnSchema::DataType FromInternalDataType(kudu::DataType type) {
  switch (type) {
    case kudu::INT8: return KuduColumnSchema::INT8;
    case kudu::INT16: return KuduColumnSchema::INT16;
    case kudu::INT32: return KuduColumnSchema::INT32;
    case kudu::INT64: return KuduColumnSchema::INT64;
    case kudu::UNIXTIME_MICROS: return KuduColumnSchema::UNIXTIME_MICROS;
    case kudu::DATE: return KuduColumnSchema::DATE;
    case kudu::FLOAT: return KuduColumnSchema::FLOAT;
    case kudu::DOUBLE: return KuduColumnSchema::DOUBLE;
    case kudu::VARCHAR: return KuduColumnSchema::VARCHAR;
    case kudu::STRING: return KuduColumnSchema::STRING;
    case kudu::BINARY: return KuduColumnSchema::BINARY;
    case kudu::BOOL: return KuduColumnSchema::BOOL;
    case kudu::DECIMAL32: return KuduColumnSchema::DECIMAL;
    case kudu::DECIMAL64: return KuduColumnSchema::DECIMAL;
    case kudu::DECIMAL128: return KuduColumnSchema::DECIMAL;
    default: LOG(FATAL) << "Unexpected internal data type: " << type;
  }
}

////////////////////////////////////////////////////////////
// KuduColumnTypeAttributes
////////////////////////////////////////////////////////////

KuduColumnTypeAttributes::KuduColumnTypeAttributes()
    : data_(new Data(0, 0, 0)) {
}

KuduColumnTypeAttributes::KuduColumnTypeAttributes(const KuduColumnTypeAttributes& other)
    : data_(nullptr) {
  CopyFrom(other);
}

KuduColumnTypeAttributes::KuduColumnTypeAttributes(int8_t precision, int8_t scale)
    : data_(new Data(precision, scale, 0)) {
}

KuduColumnTypeAttributes::KuduColumnTypeAttributes(uint16_t length)
    : data_(new Data(0, 0, length)) {
}

KuduColumnTypeAttributes::KuduColumnTypeAttributes(int8_t precision, int8_t scale, uint16_t length)
    : data_(new Data(precision, scale, length)) {
}

KuduColumnTypeAttributes::~KuduColumnTypeAttributes() {
  delete data_;
}

KuduColumnTypeAttributes& KuduColumnTypeAttributes::operator=(
    const KuduColumnTypeAttributes& other) {
  if (&other != this) {
    CopyFrom(other);
  }
  return *this;
}

void KuduColumnTypeAttributes::CopyFrom(const KuduColumnTypeAttributes& other) {
  delete data_;
  data_ = new Data(*other.data_);
}

int8_t KuduColumnTypeAttributes::precision() const {
  return data_->precision;
}

int8_t KuduColumnTypeAttributes::scale() const {
  return data_->scale;
}

uint16_t KuduColumnTypeAttributes::length() const {
  return data_->length;
}

Status KuduColumnStorageAttributes::StringToEncodingType(
    const string& encoding,
    KuduColumnStorageAttributes::EncodingType* type) {
  Status s;
  string encoding_uc;
  ToUpperCase(encoding, &encoding_uc);
  if (encoding_uc == "AUTO_ENCODING") {
    *type = KuduColumnStorageAttributes::AUTO_ENCODING;
  } else if (encoding_uc == "PLAIN_ENCODING") {
    *type = KuduColumnStorageAttributes::PLAIN_ENCODING;
  } else if (encoding_uc == "PREFIX_ENCODING") {
    *type = KuduColumnStorageAttributes::PREFIX_ENCODING;
  } else if (encoding_uc == "RLE") {
    *type = KuduColumnStorageAttributes::RLE;
  } else if (encoding_uc == "DICT_ENCODING") {
    *type = KuduColumnStorageAttributes::DICT_ENCODING;
  } else if (encoding_uc == "BIT_SHUFFLE") {
    *type = KuduColumnStorageAttributes::BIT_SHUFFLE;
  } else if (encoding_uc == "GROUP_VARINT") {
    *type = KuduColumnStorageAttributes::GROUP_VARINT;
  } else {
    s = Status::InvalidArgument(Substitute(
        "encoding type $0 is not supported", encoding));
  }
  return s;
}

Status KuduColumnStorageAttributes::StringToCompressionType(
    const string& compression,
    KuduColumnStorageAttributes::CompressionType* type) {
  Status s;
  string compression_uc;
  ToUpperCase(compression, &compression_uc);
  if (compression_uc == "DEFAULT_COMPRESSION") {
    *type = KuduColumnStorageAttributes::DEFAULT_COMPRESSION;
  } else if (compression_uc == "NO_COMPRESSION") {
    *type = KuduColumnStorageAttributes::NO_COMPRESSION;
  } else if (compression_uc == "SNAPPY") {
    *type = KuduColumnStorageAttributes::SNAPPY;
  } else if (compression_uc == "LZ4") {
    *type = KuduColumnStorageAttributes::LZ4;
  } else if (compression_uc == "ZLIB") {
    *type = KuduColumnStorageAttributes::ZLIB;
  } else {
    s = Status::InvalidArgument(Substitute(
        "compression type $0 is not supported", compression));
  }
  return s;
}

////////////////////////////////////////////////////////////
// KuduColumnSpec
////////////////////////////////////////////////////////////

KuduColumnSpec::KuduColumnSpec(const string& col_name)
  : data_(new Data(col_name)) {
}

KuduColumnSpec::~KuduColumnSpec() {
  delete data_;
}

KuduColumnSpec* KuduColumnSpec::Type(KuduColumnSchema::DataType type) {
  data_->type = type;
  return this;
}

KuduColumnSpec* KuduColumnSpec::Default(KuduValue* value) {
  if (value == nullptr) return this;
  if (data_->default_val) {
    delete data_->default_val.value();
  }
  data_->default_val = value;
  return this;
}

KuduColumnSpec* KuduColumnSpec::Compression(
    KuduColumnStorageAttributes::CompressionType compression) {
  data_->compression = compression;
  return this;
}

KuduColumnSpec* KuduColumnSpec::Encoding(
    KuduColumnStorageAttributes::EncodingType encoding) {
  data_->encoding = encoding;
  return this;
}

KuduColumnSpec* KuduColumnSpec::BlockSize(int32_t block_size) {
  data_->block_size = block_size;
  return this;
}

KuduColumnSpec* KuduColumnSpec::Precision(int8_t precision) {
  data_->precision = precision;
  return this;
}

KuduColumnSpec* KuduColumnSpec::Scale(int8_t scale) {
  data_->scale = scale;
  return this;
}

KuduColumnSpec* KuduColumnSpec::Length(uint16_t length) {
  data_->length = length;
  return this;
}

KuduColumnSpec* KuduColumnSpec::PrimaryKey() {
  data_->primary_key = true;
  return this;
}

KuduColumnSpec* KuduColumnSpec::NotNull() {
  data_->nullable = false;
  return this;
}

KuduColumnSpec* KuduColumnSpec::Nullable() {
  data_->nullable = true;
  return this;
}

KuduColumnSpec* KuduColumnSpec::RemoveDefault() {
  data_->remove_default = true;
  return this;
}

KuduColumnSpec* KuduColumnSpec::RenameTo(const string& new_name) {
  data_->rename_to = new_name;
  return this;
}

KuduColumnSpec* KuduColumnSpec::Comment(const string& comment) {
  data_->comment = comment;
  return this;
}

Status KuduColumnSpec::ToColumnSchema(KuduColumnSchema* col) const {
  // Verify that the user isn't trying to use any methods that
  // don't make sense for CREATE.
  if (data_->rename_to) {
    return Status::NotSupported("cannot rename a column during CreateTable",
                                data_->name);
  }
  if (data_->remove_default) {
    return Status::NotSupported("cannot remove default during CreateTable",
                                data_->name);
  }

  if (!data_->type) {
    return Status::InvalidArgument("no type provided for column", data_->name);
  }

  switch (data_->type.value()) {
    case KuduColumnSchema::DECIMAL:
      if (!data_->precision) {
        return Status::InvalidArgument("no precision provided for decimal column", data_->name);
      }
      if (data_->precision.value() < kMinDecimalPrecision ||
          data_->precision.value() > kMaxDecimalPrecision) {
        return Status::InvalidArgument(
            strings::Substitute("precision must be between $0 and $1",
              kMinDecimalPrecision,
              kMaxDecimalPrecision), data_->name);
      }
      if (data_->scale) {
        if (data_->scale.value() < kMinDecimalScale) {
          return Status::InvalidArgument(
              strings::Substitute("scale is less than the minimum value of $0",
                kMinDecimalScale), data_->name);
        }
        if (data_->scale.value() > data_->precision.value()) {
          return Status::InvalidArgument(
              strings::Substitute("scale is greater than the precision value of",
                data_->precision.value()), data_->name);
        }
      }
      if (data_->length) {
        return Status::InvalidArgument(
            strings::Substitute("length is not applicable for column $0",
              data_->type.value()), data_->name);
      }
      break;
    case KuduColumnSchema::VARCHAR:
      if (!data_->length) {
        return Status::InvalidArgument("no length provided for VARCHAR column", data_->name);
      }
      if (data_->length.value() < kMinVarcharLength ||
          data_->length.value() > kMaxVarcharLength) {
        return Status::InvalidArgument(
            strings::Substitute("length must be between $0 and $1",
                                kMinVarcharLength,
                                kMaxVarcharLength), data_->name);
      }
      if (data_->precision) {
        return Status::InvalidArgument(
            strings::Substitute("precision is not valid on a $0 column",
                                data_->type.value()), data_->name);
      }
      if (data_->scale) {
        return Status::InvalidArgument(
            strings::Substitute("scale is not valid on a $0 column",
                                data_->type.value()), data_->name);
      }
      break;
    default:
      if (data_->precision) {
        return Status::InvalidArgument(
            strings::Substitute("precision is not valid on a $0 column",
                                data_->type.value()), data_->name);
      }
      if (data_->scale) {
        return Status::InvalidArgument(
            strings::Substitute("scale is not valid on a $0 column",
                                data_->type.value()), data_->name);
      }
      if (data_->length) {
        return Status::InvalidArgument(
            strings::Substitute("length is not valid on a $0 column",
                                data_->type.value()), data_->name);
      }
  }

  int8_t precision = data_->precision ? data_->precision.value() : 0;
  int8_t scale = data_->scale ? data_->scale.value() : kDefaultDecimalScale;
  uint16_t length = data_->length ? data_->length.value() : 0;

  KuduColumnTypeAttributes type_attrs(precision, scale, length);
  DataType internal_type = ToInternalDataType(data_->type.value(), type_attrs);
  bool nullable = data_->nullable ? data_->nullable.value() : true;

  void* default_val = nullptr;
  // TODO(unknown): distinguish between DEFAULT NULL and no default?
  if (data_->default_val) {
    ColumnTypeAttributes internal_type_attrs(precision, scale);
    RETURN_NOT_OK(data_->default_val.value()->data_->CheckTypeAndGetPointer(
        data_->name, internal_type, internal_type_attrs,  &default_val));
  }

  // Encoding and compression.
  KuduColumnStorageAttributes::EncodingType encoding = data_->encoding ?
      data_->encoding.value() : KuduColumnStorageAttributes::AUTO_ENCODING;
  KuduColumnStorageAttributes::CompressionType compression = data_->compression ?
      data_->compression.value() : KuduColumnStorageAttributes::DEFAULT_COMPRESSION;

  // BlockSize: '0' signifies server-side default.
  int32_t block_size = data_->block_size ? data_->block_size.value() : 0;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
  *col = KuduColumnSchema(data_->name, data_->type.value(), nullable,
                          default_val,
                          KuduColumnStorageAttributes(encoding, compression, block_size),
                          type_attrs,
                          data_->comment ? data_->comment.value() : "");
#pragma GCC diagnostic pop

  return Status::OK();
}

Status KuduColumnSpec::ToColumnSchemaDelta(ColumnSchemaDelta* col_delta) const {
  if (data_->type) {
    return Status::InvalidArgument("type provided for column schema delta", data_->name);
  }
  if (data_->nullable) {
    return Status::InvalidArgument("nullability provided for column schema delta", data_->name);
  }
  if (data_->primary_key) {
    return Status::InvalidArgument("primary key set for column schema delta", data_->name);
  }

  if (data_->remove_default && data_->default_val) {
    return Status::InvalidArgument("new default set but default also removed", data_->name);
  }

  if (data_->default_val) {
    col_delta->default_value = DefaultValueAsSlice();
  }

  if (data_->remove_default) {
    col_delta->remove_default = true;
  }

  if (data_->encoding) {
    col_delta->encoding = ToInternalEncodingType(data_->encoding.value());
  }

  if (data_->compression) {
    col_delta->compression = ToInternalCompressionType(data_->compression.value());
  }

  col_delta->new_name = std::move(data_->rename_to);
  col_delta->cfile_block_size = std::move(data_->block_size);
  col_delta->new_comment = std::move(data_->comment);
  return Status::OK();
}

Slice KuduColumnSpec::DefaultValueAsSlice() const {
  if (!data_->default_val) {
    return Slice();
  }
  return data_->default_val.value()->data_->GetSlice();
}

////////////////////////////////////////////////////////////
// KuduSchemaBuilder
////////////////////////////////////////////////////////////

class KuduSchemaBuilder::Data {
 public:
  Data() {
  }

  ~Data() {
    // Rather than delete the specs here, we have to do it in
    // ~KuduSchemaBuilder(), to avoid a circular dependency in the
    // headers declaring friend classes with nested classes.
  }

  boost::optional<vector<string>> key_col_names;
  vector<KuduColumnSpec*> specs;
};

KuduSchemaBuilder::KuduSchemaBuilder()
  : data_(new Data()) {
}

KuduSchemaBuilder::~KuduSchemaBuilder() {
  for (KuduColumnSpec* spec : data_->specs) {
    // Can't use STLDeleteElements because KuduSchemaBuilder
    // is a friend of KuduColumnSpec in order to access its destructor.
    // STLDeleteElements is a free function and therefore can't access it.
    delete spec;
  }
  delete data_;
}

KuduColumnSpec* KuduSchemaBuilder::AddColumn(const string& name) {
  auto c = new KuduColumnSpec(name);
  data_->specs.push_back(c);
  return c;
}

KuduSchemaBuilder* KuduSchemaBuilder::SetPrimaryKey(
    const vector<string>& key_col_names) {
  data_->key_col_names = key_col_names;
  return this;
}

Status KuduSchemaBuilder::Build(KuduSchema* schema) {
  vector<KuduColumnSchema> cols;
  cols.resize(data_->specs.size(), KuduColumnSchema());
  for (int i = 0; i < cols.size(); i++) {
    RETURN_NOT_OK(data_->specs[i]->ToColumnSchema(&cols[i]));
  }

  int num_key_cols;

  if (!data_->key_col_names) {
    // If they didn't explicitly pass the column names for key,
    // then they should have set it on exactly one column.
    int single_key_col_idx = -1;
    for (int i = 0; i < cols.size(); i++) {
      if (data_->specs[i]->data_->primary_key) {
        if (single_key_col_idx != -1) {
          return Status::InvalidArgument("multiple columns specified for primary key",
                                         Substitute("$0, $1",
                                                    cols[single_key_col_idx].name(),
                                                    cols[i].name()));
        }
        single_key_col_idx = i;
      }
    }

    if (single_key_col_idx == -1) {
      return Status::InvalidArgument("no primary key specified");
    }

    // TODO: eventually allow primary keys which aren't the first column
    if (single_key_col_idx != 0) {
      return Status::InvalidArgument("primary key column must be the first column");
    }

    num_key_cols = 1;
  } else {
    // Build a map from name to index of all of the columns.
    unordered_map<string, int> name_to_idx_map;
    int i = 0;
    for (KuduColumnSpec* spec : data_->specs) {
      // If they did pass the key column names, then we should not have explicitly
      // set it on any columns.
      if (spec->data_->primary_key) {
        return Status::InvalidArgument("primary key specified by both SetPrimaryKey() and on a "
                                       "specific column", spec->data_->name);
      }
      // If we have a duplicate column name, the Schema::Reset() will catch it later,
      // anyway.
      name_to_idx_map[spec->data_->name] = i++;
    }

    // Convert the key column names to a set of indexes.
    vector<int> key_col_indexes;
    for (const string& key_col_name : data_->key_col_names.value()) {
      int idx;
      if (!FindCopy(name_to_idx_map, key_col_name, &idx)) {
        return Status::InvalidArgument("primary key column not defined", key_col_name);
      }
      key_col_indexes.push_back(idx);
    }

    // Currently we require that the key columns be contiguous at the front
    // of the schema. We'll lift this restriction later -- hence the more
    // flexible user-facing API.
    for (int i = 0; i < key_col_indexes.size(); i++) {
      if (key_col_indexes[i] != i) {
        return Status::InvalidArgument("primary key columns must be listed first in the schema",
                                       data_->key_col_names.value()[i]);
      }
    }

    num_key_cols = key_col_indexes.size();
  }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
  RETURN_NOT_OK(schema->Reset(cols, num_key_cols));
#pragma GCC diagnostic pop

  return Status::OK();
}


////////////////////////////////////////////////////////////
// KuduColumnSchema
////////////////////////////////////////////////////////////

string KuduColumnSchema::DataTypeToString(DataType type) {
  switch (type) {
    case INT8:
      return "INT8";
    case INT16:
      return "INT16";
    case INT32:
      return "INT32";
    case INT64:
      return "INT64";
    case STRING:
      return "STRING";
    case BOOL:
      return "BOOL";
    case FLOAT:
      return "FLOAT";
    case DOUBLE:
      return "DOUBLE";
    case BINARY:
      return "BINARY";
    case UNIXTIME_MICROS:
      return "UNIXTIME_MICROS";
    case DATE:
      return "DATE";
    case DECIMAL:
      return "DECIMAL";
    case VARCHAR:
      return "VARCHAR";
  }
  LOG(FATAL) << "Unhandled type " << type;
}

 Status KuduColumnSchema::StringToDataType(
      const string& type_str, KuduColumnSchema::DataType* type) {
  Status s;
  string type_uc;
  ToUpperCase(type_str, &type_uc);
  if (type_uc == "INT8") {
    *type = INT8;
  } else if (type_uc == "INT16") {
    *type = INT16;
  } else if (type_uc == "INT32") {
    *type = INT32;
  } else if (type_uc == "INT64") {
    *type = INT64;
  } else if (type_uc == "STRING") {
    *type = STRING;
  } else if (type_uc == "BOOL") {
    *type = BOOL;
  } else if (type_uc == "FLOAT") {
    *type = FLOAT;
  } else if (type_uc == "DOUBLE") {
    *type = DOUBLE;
  } else if (type_uc == "BINARY") {
    *type = BINARY;
  } else if (type_uc == "UNIXTIME_MICROS") {
    *type = UNIXTIME_MICROS;
  } else if (type_uc == "DECIMAL") {
    *type = DECIMAL;
  } else if (type_uc == "VARCHAR") {
    *type = VARCHAR;
  } else if (type_uc == "DATE") {
    *type = DATE;
  } else {
    s = Status::InvalidArgument(Substitute(
        "data type $0 is not supported", type_str));
  }
  return s;
}

KuduColumnSchema::KuduColumnSchema(const string &name,
                                   DataType type,
                                   bool is_nullable,
                                   const void* default_value,
                                   const KuduColumnStorageAttributes& storage_attributes,
                                   const KuduColumnTypeAttributes& type_attributes,
                                   const string& comment) {
  ColumnStorageAttributes attr_private;
  attr_private.encoding = ToInternalEncodingType(storage_attributes.encoding());
  attr_private.compression = ToInternalCompressionType(
      storage_attributes.compression());
  ColumnTypeAttributes type_attr_private;
  type_attr_private.precision = type_attributes.precision();
  type_attr_private.scale = type_attributes.scale();
  type_attr_private.length = type_attributes.length();
  col_ = new ColumnSchema(name, ToInternalDataType(type, type_attributes),
                          is_nullable,
                          default_value, default_value, attr_private,
                          type_attr_private, comment);
}

KuduColumnSchema::KuduColumnSchema(const KuduColumnSchema& other)
  : col_(nullptr) {
  CopyFrom(other);
}

KuduColumnSchema::KuduColumnSchema() : col_(nullptr) {
}

KuduColumnSchema::~KuduColumnSchema() {
  delete col_;
}

KuduColumnSchema& KuduColumnSchema::operator=(const KuduColumnSchema& other) {
  if (&other != this) {
    CopyFrom(other);
  }
  return *this;
}

void KuduColumnSchema::CopyFrom(const KuduColumnSchema& other) {
  delete col_;
  if (other.col_) {
    col_ = new ColumnSchema(*other.col_);
  } else {
    col_ = nullptr;
  }
}

bool KuduColumnSchema::Equals(const KuduColumnSchema& other) const {
  return *this == other;
}

bool KuduColumnSchema::operator==(const KuduColumnSchema& rhs) const {
  return this == &rhs || col_ == rhs.col_ ||
    (col_ != nullptr && col_->Equals(*rhs.col_, ColumnSchema::COMPARE_ALL));
}

bool KuduColumnSchema::operator!=(const KuduColumnSchema& rhs) const {
  return !(*this == rhs);
}

const string& KuduColumnSchema::name() const {
  return DCHECK_NOTNULL(col_)->name();
}

bool KuduColumnSchema::is_nullable() const {
  return DCHECK_NOTNULL(col_)->is_nullable();
}

KuduColumnSchema::DataType KuduColumnSchema::type() const {
  return FromInternalDataType(DCHECK_NOTNULL(col_)->type_info()->type());
}

KuduColumnTypeAttributes KuduColumnSchema::type_attributes() const {
  ColumnTypeAttributes type_attributes = DCHECK_NOTNULL(col_)->type_attributes();
  return KuduColumnTypeAttributes(type_attributes.precision, type_attributes.scale,
                                  type_attributes.length);
}

KuduColumnStorageAttributes KuduColumnSchema::storage_attributes() const {
  ColumnStorageAttributes storage_attributes = DCHECK_NOTNULL(col_)->attributes();
  KuduColumnStorageAttributes::EncodingType encoding_type;
  KuduColumnStorageAttributes::StringToEncodingType(
      kudu::EncodingType_Name(storage_attributes.encoding),
      &encoding_type);
  KuduColumnStorageAttributes::CompressionType compression_type;
  KuduColumnStorageAttributes::StringToCompressionType(
      kudu::CompressionType_Name(storage_attributes.compression),
      &compression_type);
  return KuduColumnStorageAttributes(encoding_type, compression_type,
                                     storage_attributes.cfile_block_size);
}

const string& KuduColumnSchema::comment() const {
  return DCHECK_NOTNULL(col_)->comment();
}

////////////////////////////////////////////////////////////
// KuduSchema
////////////////////////////////////////////////////////////

KuduSchema::KuduSchema()
  : schema_(nullptr) {
}

KuduSchema::KuduSchema(const KuduSchema& other)
  : schema_(nullptr) {
  CopyFrom(other);
}

KuduSchema::KuduSchema(const Schema& schema)
  : schema_(new Schema(schema)) {
}

KuduSchema::KuduSchema(Schema&& schema)
  : schema_(new Schema(schema)) {
}

KuduSchema::~KuduSchema() {
  delete schema_;
}

KuduSchema& KuduSchema::operator=(const KuduSchema& other) {
  if (&other != this) {
    CopyFrom(other);
  }
  return *this;
}

void KuduSchema::CopyFrom(const KuduSchema& other) {
  delete schema_;
  schema_ = new Schema(*other.schema_);
}

Status KuduSchema::Reset(const vector<KuduColumnSchema>& columns, int key_columns) {
  vector<ColumnSchema> cols_private;
  cols_private.reserve(columns.size());
  for (const auto& col : columns) {
    cols_private.emplace_back(*col.col_);
  }
  unique_ptr<Schema> new_schema(new Schema());
  RETURN_NOT_OK(new_schema->Reset(cols_private, key_columns));

  delete schema_;
  schema_ = new_schema.release();
  return Status::OK();
}

bool KuduSchema::operator==(const KuduSchema& rhs) const {
  return this == &rhs || (schema_ && rhs.schema_ && (*schema_ == *rhs.schema_));
}

bool KuduSchema::operator!=(const KuduSchema& rhs) const {
  return !(*this == rhs);
}

bool KuduSchema::Equals(const KuduSchema& other) const {
  return *this == other;
}

KuduColumnSchema KuduSchema::Column(size_t idx) const {
  const ColumnSchema& col = schema_->column(idx);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
  KuduColumnStorageAttributes attrs(FromInternalEncodingType(col.attributes().encoding),
                                    FromInternalCompressionType(col.attributes().compression));
#pragma GCC diagnostic pop
  KuduColumnTypeAttributes type_attrs(col.type_attributes().precision, col.type_attributes().scale,
                                      col.type_attributes().length);
  return KuduColumnSchema(col.name(), FromInternalDataType(col.type_info()->type()),
                          col.is_nullable(), col.read_default_value(),
                          attrs, type_attrs, col.comment());
}

bool KuduSchema::HasColumn(const std::string& col_name, KuduColumnSchema* col_schema) const {
  int idx = schema_->find_column(col_name);
  if (idx == Schema::kColumnNotFound) {
    return false;
  }
  *col_schema = Column(idx);
  return true;
}

KuduPartialRow* KuduSchema::NewRow() const {
  return new KuduPartialRow(schema_);
}

size_t KuduSchema::num_columns() const {
  return schema_->num_columns();
}

size_t KuduSchema::num_key_columns() const {
  return schema_->num_key_columns();
}

void KuduSchema::GetPrimaryKeyColumnIndexes(vector<int>* indexes) const {
  indexes->clear();
  indexes->resize(num_key_columns());
  for (int i = 0; i < num_key_columns(); i++) {
    (*indexes)[i] = i;
  }
}

string KuduSchema::ToString() const {
  return schema_ ? schema_->ToString(FLAGS_show_attributes ?
                                     Schema::ToStringMode::WITH_COLUMN_ATTRIBUTES
                                     : Schema::ToStringMode::BASE_INFO)
                 : "()";
}

KuduSchema KuduSchema::FromSchema(const Schema& schema) {
  return KuduSchema(schema.CopyWithoutColumnIds());
}

Schema KuduSchema::ToSchema(const KuduSchema& kudu_schema) {
  return Schema(*kudu_schema.schema_);
}

} // namespace client
} // namespace kudu
