/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2022/07/05.
//

#include "sql/expr/tuple_cell.h"
#include "storage/common/field.h"
#include "common/log/log.h"
#include "util/comparator.h"

void TupleCell::to_string(std::ostream &os) const
{
  switch (attr_type_) {
  case INTS: {
    os << *(int *)data_;
  } break;
  case FLOATS: {
    os << *(float *)data_;
  } break;
  case CHARS: {
    for (int i = 0; i < length_; i++) {
      if (data_[i] == '\0') {
        break;
      }
      os << data_[i];
    }
  } break;
  case DATES: {
    int value = *(int*)data_;
    char buf[16] = {0};
    snprintf(buf,sizeof(buf), "%04d-%02d-%02d", value/10000, (value%10000)/100, value%100); // 注意这里月份和天数，不足两位时需要填充0
    os << buf;
  }break;
  default: {
    LOG_WARN("unsupported attr type: %d", attr_type_);
  } break;
  }
}

void TupleCell::to_string(std::string &row, FieldMeta * field_meta)
{ 
  size_t record_size = field_meta->len();
  //char* record = new char[record_size];
  switch (attr_type_) {
  case INTS: {
    //memcpy(record, *(int *)data_, record_size );
    row += std::to_string(*(int *)data_);
  } break;
  case FLOATS: {
    row += (std::to_string(*(float *)data_));
    //memcpy(record, (int *)data_, record_size );
  } break;
  case CHARS: {
    /*
    for (int i = 0; i < length_; i++) {
      if (data_[i] == '\0') {
        break;
      }
      os << data_[i];
    }
    */
   row += (std::string(data_));
   //memcpy(record, (int *)data_, record_size );
  } break;
  default: {
    LOG_WARN("unsupported attr type: %d", attr_type_);
  } break;
  }
  //row += std::string(record);
  //delete record;
  //record = nullptr;
}

void TupleCell::make_row_record(char *record, FieldMeta *field_meta, int trx_len) {
  size_t copy_len = field_meta->len();
  size_t offset = field_meta->offset()-trx_len;
  //char* record = new char[record_size];
  switch (attr_type_) {
  case INTS: {
    memcpy(record + offset, data_, copy_len);
    //row += std::to_string(*(int *)data_);
  } break;
  case FLOATS: {
    //row += (std::to_string(*(float *)data_));
    memcpy(record + offset, data_, copy_len);
  } break;
  case CHARS: {
    const size_t data_len = strlen((const char *)data_);
    if (copy_len > data_len) {  //in case of size overflow?
      copy_len = data_len + 1;
    }
   //row += (std::string(data_));
   memcpy(record + offset, data_, copy_len);
  } break;
  default: {
    LOG_WARN("unsupported attr type: %d", attr_type_);
  } break;
  }
}

int TupleCell::compare(const TupleCell &other) const
{
  if (this->attr_type_ == other.attr_type_) {
    switch (this->attr_type_) {
    case INTS: return compare_int(this->data_, other.data_);
    case FLOATS: return compare_float(this->data_, other.data_);
    case CHARS: return compare_string(this->data_, this->length_, other.data_, other.length_);
    case DATES: return compare_int(this->data_, other.data_);
    default: {
      LOG_WARN("unsupported type: %d", this->attr_type_);
    }
    }
  } else if (this->attr_type_ == INTS && other.attr_type_ == FLOATS) {
    float this_data = *(int *)data_;
    return compare_float(&this_data, other.data_);
  } else if (this->attr_type_ == FLOATS && other.attr_type_ == INTS) {
    float other_data = *(int *)other.data_;
    return compare_float(data_, &other_data);
  }
  LOG_WARN("not supported");
  return -1; // TODO return rc?
}
