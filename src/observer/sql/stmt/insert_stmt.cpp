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
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/insert_stmt.h"
#include "common/log/log.h"
#include "storage/common/db.h"
#include "storage/common/table.h"
#include "util/date.h"
#include "sql/parser/parse_defs.h"
#include "storage/common/record.h"
#include "storage/common/record_manager.h"

InsertStmt::InsertStmt(Table *table, const Inserts &inserts)
  : table_ (table), inserts_(inserts)
{}

RC InsertStmt::create(Db *db, const Inserts &inserts, Stmt *&stmt)
{
  const char *table_name = inserts.relation_name;
  if (nullptr == db || nullptr == table_name ) {
    LOG_WARN("invalid argument. db=%p, table_name=%p", 
             db, table_name);
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  //get unique indexmeta if exists
  const TableMeta &table_meta = table->table_meta();
  bool table_has_unique = false;
  const IndexMeta *index_meta = nullptr;
  for(int i=0,n=table_meta.index_num(); i<n; i++) {
    const IndexMeta *index_meta_;
    index_meta_ = table_meta.index(i);
    if(index_meta_->isUnique_ == 1) {
      index_meta = index_meta_;
      table_has_unique = true;
      break;
    }
  }

  //get unique field_offset and unique set if exists
  int field_offset = 0;
  std::set<int> u_set;
  if(table_has_unique) {
    std::string ss = index_meta->field();
    const FieldMeta *field_meta = table_meta.field(ss.c_str());
    field_offset = field_meta->offset();

    //get unique set
    DiskBufferPool *data_buffer_pool = table->getDiskBufferPool();
    if(data_buffer_pool == nullptr)
      return RC::INVALID_ARGUMENT;
    RecordFileScanner scanner;
    RC rc = scanner.open_scan(*data_buffer_pool, nullptr);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("failed to open scanner. rc=%d:%s", rc, strrc(rc));
      return rc;
    }
    Record record;
    while (scanner.has_next()) {
     rc = scanner.next(record);
     if (rc != RC::SUCCESS) {
       LOG_WARN("failed to fetch next record. rc=%d:%s", rc, strrc(rc));
       return rc;
     }
     int rec = *(int*)(record.data_+field_offset);
     if (u_set.count(rec)>0) {
       return RC::NOT_UNIQUE_ERR;
      } 
      else {
       u_set.insert(rec);
      }
    }
  }

  // check the fields number
  //const Value *values = inserts.values;
  //const int value_num = inserts.value_num;
  int count = inserts.insert_num;
  std::set<int> in_set;
  for(int i=0; i<count; i++) {
  const Value *values = inserts.insertValue[i].values;
  const int value_num = inserts.insertValue[i].value_num;
  if(value_num <= 0) {
    return RC::INVALID_ARGUMENT;
  }
  //const TableMeta &table_meta = table->table_meta();
  const int field_num = table_meta.field_num() - table_meta.sys_field_num();
  if (field_num != value_num) {
    LOG_WARN("schema mismatch. value num=%d, field num in schema=%d", value_num, field_num);
    return RC::SCHEMA_FIELD_MISSING;
  }

  // check fields type
  const int sys_field_num = table_meta.sys_field_num();
  for (int i = 0; i < value_num; i++) {
    const FieldMeta *field_meta = table_meta.field(i + sys_field_num);
    const char *field_name = field_meta->name();     
    const AttrType field_type = field_meta->type();
    const AttrType value_type = values[i].type;
    
    if (table_has_unique && index_meta != nullptr && strcmp(field_name, index_meta->field()) == 0) {
      int t = *(int*)values[i].data;
      if(in_set.count(t) > 0) {
        return RC::NOT_UNIQUE_ERR;
      }
      else {
        in_set.insert(t);
      }
      if(u_set.count(t)> 0) {
        return RC::NOT_UNIQUE_ERR;
      }
    }

    if (field_type != value_type) { // TODO try to convert the value type to field type
      if (field_type == DATES) {
        int32_t date = -1;
        RC rc = string_to_date((char*)values[i].data, date);
        if (rc != RC::SUCCESS) {
          LOG_TRACE("this is not a date,insert_stmt.cpp");
          return rc;
        }
        //value_destroy(&values[i]);
        //value_init_date(&values[i], date);
        value_destroy((Value*)&values[i]);
        value_init_date((Value*)&values[i], date);
      }
      else{
      	LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d", 
               table_name, field_meta->name(), field_type, value_type);
      	return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
    }
  }
  }
  // everything alright
  //stmt = new InsertStmt(table, values, value_num);
  stmt = new InsertStmt(table, inserts);
  return RC::SUCCESS;
}
