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

#pragma once

#include "sql/operator/operator.h"
#include "sql/expr/tuple.h"

class FilterStmt;
class TupleSet;
class CompositeTuple;
class CompositeConditionFilter;
/**
 * PredicateOperator 用于单个表中的记录过滤
 * 如果是多个表数据过滤，比如join条件的过滤，需要设计新的predicate或者扩展:w
 */
class PredicateOperator : public Operator
{
public:
  PredicateOperator(FilterStmt *filter_stmt)
    : filter_stmt_(filter_stmt)
  {}

  virtual ~PredicateOperator() = default;

  RC open() override;
  RC next() override;
  RC close() override;

  Tuple * current_tuple() override;
  //int tuple_cell_num() const override;
  //RC tuple_cell_spec_at(int index, TupleCellSpec &spec) const override;
private:
  bool do_predicate(RowTuple &tuple);
private:
  FilterStmt *filter_stmt_ = nullptr;
};
/*
class MultiPredicateOperator : public Operator
{
public:
  MultiPredicateOperator(FilterStmt *filter_stmt)
    : filter_stmt_(filter_stmt)
  {}

  virtual ~MultiPredicateOperator() = default;

  RC open() override;
  RC next() override;
  RC close() override;

  Tuple * current_tuple() override;
  void index_add() { tuple_set_index++;}
  //int tuple_cell_num() const override;
  //RC tuple_cell_spec_at(int index, TupleCellSpec &spec) const override;
private:
  bool do_multiple_predicate(CompositeTuple &tuple);
private:
  //Record current_record_;
  Tuple *current_record_;

  int tuple_set_index = 0; //MergeTableScannerIndex
  int merge_scan_ = 0; //0 means not merge multiple tables yet ,and 1 means already merge
  TupleSet  tuple_set_;
  
  FilterStmt *filter_stmt_ = nullptr;

  //std::vector<TupleSet> multi_tuple_sets;
};
*/
void multi_to_string(char* data, int offset, int length_, AttrType attr_type_,std::ostream &os);
void DescartestRecursive(std::vector<std::vector<char *> >& originalList, int position,  char *line, 
CompositeConditionFilter &composite_condition_filter, std::vector<int> multiple_table_record_sizes,
 std::ostream& os, std::vector<FieldMeta>& multi_field_table); 

void getDescartes(std::vector< std::vector<char *> >& originalList, CompositeConditionFilter &composite_condition_filter,std::vector<int> multiple_table_record_sizes, std::ostream& os, std::vector<FieldMeta>& multi_field_table);