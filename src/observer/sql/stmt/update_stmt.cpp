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

#include "sql/stmt/update_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "common/log/log.h"
#include "common/lang/string.h"
#include "storage/common/db.h"
#include "storage/common/table.h"

UpdateStmt::UpdateStmt(Table *table, Value *values, int value_amount)
  : table_ (table), values_(values), value_amount_(value_amount)
{}

RC UpdateStmt::create(Db *db, const Updates &update, Stmt *&stmt)
{
  // TODO
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  } 

  // collect tables in `update` statement
  std::vector<Table *> tables;
  std::unordered_map<std::string, Table *> table_map;
  const char *table_name = update.relation_name;
  if (nullptr == table_name) {
    LOG_WARN("invalid argument. relation name is null.");
    return RC::INVALID_ARGUMENT;
  }

  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(),table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  tables.push_back(table);
  table_map.insert(std::pair<std::string, Table*>(table_name, table));

  //collect query fields and value in `set` statement
  // to do
  std::vector<Field> query_fields;
  const char *attribute_name = update.attribute_name;
  
  if (!common::is_blank(attribute_name)) {
    const char *field_name = attribute_name;
    const FieldMeta *field_meta = table->table_meta().field(field_name);
    if (nullptr == field_meta) {
      LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), field_name);
      return RC::SCHEMA_FIELD_NOT_EXIST;
    }
    query_fields.push_back(Field(table, field_meta));
  } else {
    LOG_WARN("invalid. The field name is empty.");
    return RC::SCHEMA_FIELD_MISSING;
  }

  Table *default_table = nullptr;
  if (tables.size()==1) {
    default_table = tables[0];
  }

  // check fields type
  const Value *values = &update.value;    
  const TableMeta &table_meta = table->table_meta();  
  //const int sys_field_num = table_meta.sys_field_num();
  const FieldMeta *field_meta = table_meta.field(update.attribute_name);
    const AttrType field_type = field_meta->type();
    const AttrType value_type = values->type;
    if (field_type != value_type) { // TODO try to convert the value type to field type
      LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d", 
               table_name, field_meta->name(), field_type, value_type);
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
  



  //create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  RC rc = FilterStmt::create(db, default_table, &table_map, update.conditions, update.condition_num, filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt.");
    return rc;
  }

  //everything alright

  UpdateStmt *update_stmt = new UpdateStmt();
  update_stmt->table_ = table;
  update_stmt->values_ = (Value *)&update.value;  //how to write?
  update_stmt->value_amount_ = 1;
  update_stmt->attribute_name_ = (char *)query_fields[0].field_name();
  update_stmt->filter_stmt_ = filter_stmt;
  stmt = update_stmt;
  return RC::SUCCESS;
}
