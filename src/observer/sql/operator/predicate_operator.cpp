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
// Created by WangYunlai on 2022/6/27.
//

#include "common/log/log.h"
#include "sql/operator/predicate_operator.h"
#include "storage/common/record.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/common/field.h"
#include "storage/common/condition_filter.h"
#include <typeinfo>
#include <iostream>

class FieldMeta;

RC PredicateOperator::open()
{
  if (children_.size() != 1) {
    LOG_WARN("predicate operator must has one child");
    return RC::INTERNAL;
  }

  return children_[0]->open();
}

RC PredicateOperator::next()
{
  RC rc = RC::SUCCESS;
  Operator *oper = children_[0];
  
  while (RC::SUCCESS == (rc = oper->next())) {
    Tuple *tuple = oper->current_tuple();
    if (nullptr == tuple) {
      rc = RC::INTERNAL;
      LOG_WARN("failed to get tuple from operator");
      break;
    }

    if (do_predicate(static_cast<RowTuple &>(*tuple))) {
      return rc;
    }
  }
  return rc;
}

RC PredicateOperator::close()
{
  children_[0]->close();
  return RC::SUCCESS;
}

Tuple * PredicateOperator::current_tuple()
{
  return children_[0]->current_tuple();
}

bool PredicateOperator::do_predicate(RowTuple &tuple)
{
  if (filter_stmt_ == nullptr || filter_stmt_->filter_units().empty()) {
    return true;
  }

  for (const FilterUnit *filter_unit : filter_stmt_->filter_units()) {
    //for multiple table conditions, if table which the current tuple belong to is not same with the table which current filter_unit involved in , just continue recycle.
    //tuple has member `table`
    //fieldexpr has member `table`
    //compare them, if not the same , recycle continues;
    Expression *left_expr = filter_unit->left();
    Expression *right_expr = filter_unit->right();

    const char* tuple_table_name = ((RowTuple &)tuple).table_name();
    if (left_expr->type() == ExprType::FIELD ) {
      int ret = strcmp(((FieldExpr *)left_expr)->table_name(),tuple_table_name);
      if ( ret != 0) {
        continue;
      }
    }

    if (right_expr->type() == ExprType::FIELD ) {
      int ret = strcmp(((FieldExpr *)right_expr)->table_name(),tuple_table_name);
      if ( ret != 0) {
        continue;
      }
    }

    //if (left_expr->)
    //if ()

    CompOp comp = filter_unit->comp();
    TupleCell left_cell;
    TupleCell right_cell;
    left_expr->get_value(tuple, left_cell);
    right_expr->get_value(tuple, right_cell);

    const int compare = left_cell.compare(right_cell);
    bool filter_result = false;
    switch (comp) {
    case EQUAL_TO: {
      filter_result = (0 == compare); 
    } break;
    case LESS_EQUAL: {
      filter_result = (compare <= 0); 
    } break;
    case NOT_EQUAL: {
      filter_result = (compare != 0);
    } break;
    case LESS_THAN: {
      filter_result = (compare < 0);
    } break;
    case GREAT_EQUAL: {
      filter_result = (compare >= 0);
    } break;
    case GREAT_THAN: {
      filter_result = (compare > 0);
    } break;
    default: {
      LOG_WARN("invalid compare type: %d", comp);
    } break;
    }
    if (!filter_result) {
      return false;
    }
  }
  return true;
}

void multi_to_string(char* data, int offset, int length_, AttrType attr_type_,std::ostream &os) {
  switch (attr_type_) {
  case INTS: {
    os << *(int *)(data + offset);
  } break;
  case FLOATS: {
    os << *(float *)(data + offset);
  } break;
  case CHARS: {
    char * data_ = data + offset;
    for (int i = 0; i < length_; i++) {
      if (data_[i] == '\0') {
        break;
      }
      os << data_[i];
    }
  } break;
  default: {
    LOG_WARN("unsupported attr type: %d", attr_type_);
  } break;
  }
}

void DescartestRecursive(std::vector<std::vector<char*> >& originalList, int position, 
    char * line, CompositeConditionFilter &composite_condition_filter, std::vector<int> multiple_table_record_sizes, 
      std::ostream& os, std::vector<FieldMeta>& multi_field_table) {
  // traverse the number `position` table
  std::vector< char* > &table = originalList[position];
  size_t table_record_size = multiple_table_record_sizes[position];
  size_t table_offset = 0;
  for (int i=0; i< position;i++) {
    table_offset += multiple_table_record_sizes[i];
  }
  std::string prefix("");
  /*
  if (position != 0) {
    prefix += " | ";
  }
  */
  for (int i = 0; i < table.size(); i++) {
    memcpy(line + table_offset, table[i], table_record_size);
    //std::string tuple_tail( prefix + table[i]);
    //std::string tuple_tail(table[i]);
    //line += tuple_tail;
    //size_t start_position_to_erase = line.find(tuple_tail);
    //tuple becomes complete
    if (position == originalList.size()-1) {
      bool first = true;
      if (composite_condition_filter.string_filter(line)) {
        for (int j=0; j< multi_field_table.size(); j++) {
          if (!first) {
            os << " | ";
          }else {
            first = false;
          }
          FieldMeta tmp = multi_field_table[j];
          multi_to_string(line,tmp.offset(),tmp.len(), tmp.type(), os);
        }
        os << std::endl;
        //std::string backend(line);
        //ReturnList.push_back(backend);
      }
      memset(line+ table_offset, 0, table_record_size);
      continue;
    }

    DescartestRecursive(originalList, position+1, line, composite_condition_filter, multiple_table_record_sizes, os, multi_field_table);
    memset(line+ table_offset, 0, table_record_size);;
  }
}

//TupleSet getDescartes(std::vector<TupleSet>& list);
 void getDescartes(std::vector< std::vector<char *> >& originalList, CompositeConditionFilter &composite_condition_filter, std::vector<int> multiple_table_record_sizes, std::ostream& os, std::vector<FieldMeta>& multi_field_table ) {
  //TupleSet returnList;
  int record_size = 0;
  for (int i=0; i < multiple_table_record_sizes.size();i++) {
    record_size += multiple_table_record_sizes[i];
  }
  char * line = new char[record_size];
  DescartestRecursive(originalList, 0, line, composite_condition_filter, multiple_table_record_sizes, os, multi_field_table);
  
}