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
// Created by Meiyi & Longda on 2021/4/13.
//

#include <string>
#include <sstream>
#include <numeric>
#include <algorithm>

#include "execute_stage.h"

#include "common/io/io.h"
#include "common/log/log.h"
#include "common/lang/defer.h"
#include "common/seda/timer_stage.h"
#include "common/lang/string.h"
#include "session/session.h"
#include "event/storage_event.h"
#include "event/sql_event.h"
#include "event/session_event.h"
#include "sql/expr/tuple.h"
#include "sql/operator/table_scan_operator.h"
#include "sql/operator/index_scan_operator.h"
#include "sql/operator/predicate_operator.h"
#include "sql/operator/delete_operator.h"
#include "sql/operator/project_operator.h"
#include "sql/stmt/stmt.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/update_stmt.h"
#include "sql/stmt/delete_stmt.h"
#include "sql/stmt/insert_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/common/table.h"
#include "storage/common/field.h"
#include "storage/index/index.h"
#include "storage/default/default_handler.h"
#include "storage/common/condition_filter.h"
#include "storage/trx/trx.h"

using namespace common;

//RC create_selection_executor(
//   Trx *trx, const Selects &selects, const char *db, const char *table_name, SelectExeNode &select_node);

//! Constructor
ExecuteStage::ExecuteStage(const char *tag) : Stage(tag)
{}

//! Destructor
ExecuteStage::~ExecuteStage()
{}

//! Parse properties, instantiate a stage object
Stage *ExecuteStage::make_stage(const std::string &tag)
{
  ExecuteStage *stage = new (std::nothrow) ExecuteStage(tag.c_str());
  if (stage == nullptr) {
    LOG_ERROR("new ExecuteStage failed");
    return nullptr;
  }
  stage->set_properties();
  return stage;
}

//! Set properties for this object set in stage specific properties
bool ExecuteStage::set_properties()
{
  //  std::string stageNameStr(stageName);
  //  std::map<std::string, std::string> section = theGlobalProperties()->get(
  //    stageNameStr);
  //
  //  std::map<std::string, std::string>::iterator it;
  //
  //  std::string key;

  return true;
}

//! Initialize stage params and validate outputs
bool ExecuteStage::initialize()
{
  LOG_TRACE("Enter");

  std::list<Stage *>::iterator stgp = next_stage_list_.begin();
  default_storage_stage_ = *(stgp++);
  mem_storage_stage_ = *(stgp++);

  LOG_TRACE("Exit");
  return true;
}

//! Cleanup after disconnection
void ExecuteStage::cleanup()
{
  LOG_TRACE("Enter");

  LOG_TRACE("Exit");
}

void ExecuteStage::handle_event(StageEvent *event)
{
  LOG_TRACE("Enter\n");

  handle_request(event);

  LOG_TRACE("Exit\n");
  return;
}

void ExecuteStage::callback_event(StageEvent *event, CallbackContext *context)
{
  LOG_TRACE("Enter\n");

  // here finish read all data from disk or network, but do nothing here.

  LOG_TRACE("Exit\n");
  return;
}

void ExecuteStage::handle_request(common::StageEvent *event)
{
  SQLStageEvent *sql_event = static_cast<SQLStageEvent *>(event);
  SessionEvent *session_event = sql_event->session_event();
  Stmt *stmt = sql_event->stmt();
  Session *session = session_event->session();
  Query *sql = sql_event->query();

  if (stmt != nullptr) {
    switch (stmt->type()) {
    case StmtType::SELECT: {
      RC rc = do_select(sql_event);
      if(rc != RC::SUCCESS){
        session_event->set_response("FAILURE\n");
      }
      //exe_event->done_immediate();
    } break;
    case StmtType::INSERT: {
      do_insert(sql_event);
    } break;
    case StmtType::UPDATE: {
      //do_update(sql_event);
      do_update(sql_event);
    } break;
    case StmtType::DELETE: {
      do_delete(sql_event);
    } break;
    }
  } else {
    switch (sql->flag) {
    case SCF_HELP: {
      do_help(sql_event);
    } break;
    case SCF_CREATE_TABLE: {
      do_create_table(sql_event);
    } break;
    case SCF_CREATE_INDEX: {
      do_create_index(sql_event);
    } break;
    case SCF_SHOW_TABLES: {
      do_show_tables(sql_event);
    } break;
    case SCF_DESC_TABLE: {
      do_desc_table(sql_event);
    } break;

    case SCF_DROP_TABLE: {
      do_drop_table(sql_event);
    } break;

    case SCF_DROP_INDEX:
    case SCF_LOAD_DATA: {
      default_storage_stage_->handle_event(event);
    } break;
    case SCF_SYNC: {
      RC rc = DefaultHandler::get_default().sync();
      session_event->set_response(strrc(rc));
    } break;
    case SCF_BEGIN: {
      session_event->set_response("SUCCESS\n");
    } break;
    case SCF_COMMIT: {
      Trx *trx = session->current_trx();
      RC rc = trx->commit();
      session->set_trx_multi_operation_mode(false);
      session_event->set_response(strrc(rc));
    } break;
    case SCF_ROLLBACK: {
      Trx *trx = session_event->get_client()->session->current_trx();
      RC rc = trx->rollback();
      session->set_trx_multi_operation_mode(false);
      session_event->set_response(strrc(rc));
    } break;
    case SCF_EXIT: {
      // do nothing
      const char *response = "Unsupported\n";
      session_event->set_response(response);
    } break;
    default: {
      LOG_ERROR("Unsupported command=%d\n", sql->flag);
    }
    }
  }
}

void end_trx_if_need(Session *session, Trx *trx, bool all_right)
{
  if (!session->is_trx_multi_operation_mode()) {
    if (all_right) {
      trx->commit();
    } else {
      trx->rollback();
    }
  }
}

void print_tuple_header(std::ostream &os, const ProjectOperator &oper)
{
  const int cell_num = oper.tuple_cell_num();
  const TupleCellSpec *cell_spec = nullptr;
  for (int i = 0; i < cell_num; i++) {
    oper.tuple_cell_spec_at(i, cell_spec);
    if (i != 0) {
      os << " | ";
    }

    if (cell_spec->alias()) {
      os << cell_spec->alias();
    }
  }

  if (cell_num > 0) {
    os << '\n';
  }
}

void make_multiple_tuple_header(std::string& headerlist, std::vector<FieldMeta>& multi_project_tuplecell)
{
  int cell_num = multi_project_tuplecell.size();

  for (int i = 0; i < cell_num; i++) {
    const char* field_name = multi_project_tuplecell[i].name();
    if (i != 0) {
      headerlist += " | ";
    }

      headerlist += std::string(field_name);
  }

}
void tuple_to_string(std::ostream &os, const Tuple &tuple)
{
  TupleCell cell;
  RC rc = RC::SUCCESS;
  bool first_field = true;
  for (int i = 0; i < tuple.cell_num(); i++) {
    rc = tuple.cell_at(i, cell);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to fetch field of cell. index=%d, rc=%s", i, strrc(rc));
      break;
    }

    if (!first_field) {
      os << " | ";
    } else {
      first_field = false;
    }
    cell.to_string(os);
  }
}

void store_string_tuple(const Tuple &tuple, std::string &row)
{
  TupleCell cell;
  const TupleCellSpec *tuple_cell_spec = nullptr;
  RC rc = RC::SUCCESS;
  bool first_field = true;
  for (int i = 0; i < tuple.cell_num(); i++) {
    rc = tuple.cell_at(i, cell);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to fetch field of cell. index=%d, rc=%s", i, strrc(rc));
      break;
    }
    if (!first_field) {
      row += " | ";
    } else {
      first_field = false;
    }
    //ProjectTuple & project_tuple = (ProjectTuple &)tuple;
    tuple.cell_spec_at(i,tuple_cell_spec);
    FieldExpr * field_expr = (FieldExpr *)tuple_cell_spec->expression();
    const FieldMeta * field_meta= field_expr->field().meta();
    cell.to_string(row, (FieldMeta *)field_meta);
  }  
}

/*
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
*/
void store_tuple(const Tuple &tuple, char * record, int trx_len)
{
  TupleCell cell;
  const TupleCellSpec *tuple_cell_spec = nullptr;
  RC rc = RC::SUCCESS;
  //bool first_field = true;
  for (int i = 0; i < tuple.cell_num(); i++) {
    rc = tuple.cell_at(i, cell);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to fetch field of cell. index=%d, rc=%s", i, strrc(rc));
      break;
    }
    //multi_table_project_cell.push_back(cell);
    //ProjectTuple & project_tuple = (ProjectTuple &)tuple;
    tuple.cell_spec_at(i,tuple_cell_spec);
    FieldExpr * field_expr = (FieldExpr *)tuple_cell_spec->expression();
    const FieldMeta * field_meta= field_expr->field().meta();
    /*
    FieldMeta tmp(*field_meta);
    int single_offset = tmp.offset();
    int multi_offset = single_offset + multiple_record_size-trx_len;
    tmp.set_offset(multi_offset);
    multi_table_project_cell.push_back(tmp);
    */
    cell.make_row_record(record, (FieldMeta *)field_meta, trx_len);
  }  
}

void make_full_record(Tuple *tuple, char *record, int record_size, int trx_len) {
  RowTuple *row_tuple = (RowTuple *)((ProjectTuple *)tuple)->get_row_tuple();
  Record full_record = row_tuple->record();
  //Tuple * tmp =((ProjectTuple &)tuple).get_row_tuple();
  char * data = full_record.data() + trx_len;
  memcpy(record , data, record_size);
}


IndexScanOperator *try_to_create_index_scan_operator(FilterStmt *filter_stmt)
{
  const std::vector<FilterUnit *> &filter_units = filter_stmt->filter_units();
  if (filter_units.empty() ) {
    return nullptr;
  }

  // 在所有过滤条件中，找到字段与值做比较的条件，然后判断字段是否可以使用索引
  // 如果是多列索引，这里的处理需要更复杂。
  // 这里的查找规则是比较简单的，就是尽量找到使用相等比较的索引
  // 如果没有就找范围比较的，但是直接排除不等比较的索引查询. (你知道为什么?)
  const FilterUnit *better_filter = nullptr;
  for (const FilterUnit * filter_unit : filter_units) {
    if (filter_unit->comp() == NOT_EQUAL) {
      continue;
    }

    Expression *left = filter_unit->left();
    Expression *right = filter_unit->right();
    if (left->type() == ExprType::FIELD && right->type() == ExprType::VALUE) {
    } else if (left->type() == ExprType::VALUE && right->type() == ExprType::FIELD) {
      std::swap(left, right);
    }
    FieldExpr &left_field_expr = *(FieldExpr *)left;
    const Field &field = left_field_expr.field();
    const Table *table = field.table();
    Index *index = table->find_index_by_field(field.field_name());
    if (index != nullptr) {
      if (better_filter == nullptr) {
        better_filter = filter_unit;
      } else if (filter_unit->comp() == EQUAL_TO) {
        better_filter = filter_unit;
    	break;
      }
    }
  }

  if (better_filter == nullptr) {
    return nullptr;
  }

  Expression *left = better_filter->left();
  Expression *right = better_filter->right();
  CompOp comp = better_filter->comp();
  if (left->type() == ExprType::VALUE && right->type() == ExprType::FIELD) {
    std::swap(left, right);
    switch (comp) {
    case EQUAL_TO:    { comp = EQUAL_TO; }    break;
    case LESS_EQUAL:  { comp = GREAT_THAN; }  break;
    case NOT_EQUAL:   { comp = NOT_EQUAL; }   break;
    case LESS_THAN:   { comp = GREAT_EQUAL; } break;
    case GREAT_EQUAL: { comp = LESS_THAN; }   break;
    case GREAT_THAN:  { comp = LESS_EQUAL; }  break;
    default: {
    	LOG_WARN("should not happen");
    }
    }
  }


  FieldExpr &left_field_expr = *(FieldExpr *)left;
  const Field &field = left_field_expr.field();
  const Table *table = field.table();
  Index *index = table->find_index_by_field(field.field_name());
  assert(index != nullptr);

  ValueExpr &right_value_expr = *(ValueExpr *)right;
  TupleCell value;
  right_value_expr.get_tuple_cell(value);

  const TupleCell *left_cell = nullptr;
  const TupleCell *right_cell = nullptr;
  bool left_inclusive = false;
  bool right_inclusive = false;

  switch (comp) {
  case EQUAL_TO: {
    left_cell = &value;
    right_cell = &value;
    left_inclusive = true;
    right_inclusive = true;
  } break;

  case LESS_EQUAL: {
    left_cell = nullptr;
    left_inclusive = false;
    right_cell = &value;
    right_inclusive = true;
  } break;

  case LESS_THAN: {
    left_cell = nullptr;
    left_inclusive = false;
    right_cell = &value;
    right_inclusive = false;
  } break;

  case GREAT_EQUAL: {
    left_cell = &value;
    left_inclusive = true;
    right_cell = nullptr;
    right_inclusive = false;
  } break;

  case GREAT_THAN: {
    left_cell = &value;
    left_inclusive = false;
    right_cell = nullptr;
    right_inclusive = false;
  } break;

  default: {
    LOG_WARN("should not happen. comp=%d", comp);
  } break;
  }

  IndexScanOperator *oper = new IndexScanOperator(table, index,
       left_cell, left_inclusive, right_cell, right_inclusive);

  LOG_INFO("use index for scan: %s in table %s", index->index_meta().name(), table->name());
  return oper;
}

RC ExecuteStage::do_select(SQLStageEvent *sql_event)
{
  SelectStmt *select_stmt = (SelectStmt *)(sql_event->stmt());
  SessionEvent *session_event = sql_event->session_event();
  RC rc = RC::SUCCESS;
  if (select_stmt->tables().size() != 1) {
    
    //scan_record(Trx *trx, ConditionFilter *filter,int limit, void *context, void (*record_reader)(const char *data, void *context));
    
    //std::vector< std::vector<std::string> > vec;
    std::vector< std::vector< char * > > vec;
    std::stringstream ss;
    std::string tuple_header_list;

    FilterStmt *filter_stmt = select_stmt->filter_stmt();
    const std::vector<Table *>& table_list = select_stmt->tables();
    const std::vector<FilterUnit *>& filter_units = filter_stmt->filter_units();
    int filter_num = 0; //the size of composite conditions

    int condition_num = filter_units.size(); 
    ConditionFilter **condition_filters = new ConditionFilter *[condition_num];
    DefaultConditionFilter * default_condition_filter = nullptr;
    //collect the multiple conditions
    for (int i=0; i < filter_units.size(); i++) {
      FilterUnit * filter_unit = filter_units[i];
      Expression *expr_left = filter_unit->left();
      Expression *expr_right = filter_unit->right();
      //select composite table conditions
      if (expr_left->type()!=ExprType::FIELD || expr_right->type()!=ExprType::FIELD) {
        continue;
      }

      //calculate multiple offset
      int table_index = 0;
      int multiple_offset_left = 0;
      int multiple_offset_right = 0;
      const char* table_of_field_left = ((FieldExpr *)expr_left)->table_name();
      for (int i=table_list.size()-1; i >= 0; i--) {        
        if (0 == strcmp(table_of_field_left, table_list[i]->name())){
          //table_index = i;
          break;
        } else {
          int table_first_field_trx_length = table_list[i]->table_meta().field_metas()->front().offset();
          int table_last_field_offset = table_list[i]->table_meta().field_metas()->back().offset();
          int table_last_field_length = table_list[i]->table_meta().field_metas()->back().len();
          int next_table_start_offset = table_last_field_offset + table_last_field_length - table_first_field_trx_length;
          multiple_offset_left += next_table_start_offset;
        }    
      }

      const char* table_of_field_right = ((FieldExpr *)expr_right)->table_name();
      for (int i = table_list.size()-1; i >= 0; i--) {        
        if (0 == strcmp(table_of_field_right, table_list[i]->name())){
          //table_index = i;
          break;
        } else {
          int table_first_field_trx_length = table_list[i]->table_meta().field_metas()->front().offset();
          int table_last_field_offset = table_list[i]->table_meta().field_metas()->back().offset();
          int table_last_field_length = table_list[i]->table_meta().field_metas()->back().len();
          int next_table_start_offset = table_last_field_offset + table_last_field_length - table_first_field_trx_length;
          multiple_offset_right += next_table_start_offset;
        }    
      }
      std::cout << table_of_field_left << std::endl;
      std::cout << table_of_field_right << std::endl;

      ConDesc left;
      left.is_attr = true;
      left.attr_length = ((FieldExpr *)expr_left)->field().meta()->len();
      //left.attr_offset = ((FieldExpr *)expr_left)->field().meta()->offset();
      left.attr_offset = ((FieldExpr *)expr_left)->field().meta()->offset() + multiple_offset_left;

      ConDesc right;
      right.is_attr = true;
      right.attr_length = ((FieldExpr *)expr_right)->field().meta()->len();
      //right.attr_offset = ((FieldExpr *)expr_right)->field().meta()->offset(); 
      right.attr_offset = ((FieldExpr *)expr_right)->field().meta()->offset() + multiple_offset_right;
      //do we need to confirm if expr_left.attr_type is same with expr_right.attr_type? 
      AttrType attr_type = ((FieldExpr *)expr_left)->field().attr_type();
      CompOp comp_op = filter_units[i]->comp();

      default_condition_filter = new DefaultConditionFilter();
      default_condition_filter->init(left, right, attr_type, comp_op);
      //condition_filter->init();

      condition_filters[filter_num] = default_condition_filter;
      filter_num += 1;
      //multiple_condition_index += 1;
    }

    CompositeConditionFilter composite_condition_filter;
    composite_condition_filter.init((const ConditionFilter **)condition_filters,filter_num);
    std::vector<int> multiple_table_record_sizes; 
    int multiple_record_size = 0;
    int trx_len = select_stmt->tables()[0]->table_meta().field_metas()->front().offset();
    std::vector<FieldMeta> multiple_table_fieldmeta;
    std::vector<FieldMeta> multiple_table_project_cell;
    //scan each table  
    for (int i= select_stmt->tables().size()-1; i >= 0; i--) {
      //TupleSet tupleset;
      std::vector<char *> tuple_set_;
      const Table * current_scan_table = select_stmt->tables()[i];


      int record_size = current_scan_table->table_meta().record_size();
      //multiple_record_size += record_size;
      const std::vector<FieldMeta>& field_meta = current_scan_table->table_meta().field_meta();
      for (int i = 0; i < field_meta.size(); i++) {
        if (strcmp(field_meta[i].name(),"__trx") != 0) {
          int single_offset = field_meta[i].offset();
          int multiple_offset = single_offset + multiple_record_size-trx_len;
          FieldMeta fieldmeta(field_meta[i]);
          fieldmeta.set_offset(multiple_offset);
          multiple_table_fieldmeta.push_back(fieldmeta);
        }
      }   

      int current_table_trx_length = current_scan_table->table_meta().field_metas()->front().offset();
      //trx_len = current_table_trx_length;
      Operator *scan_oper = try_to_create_index_scan_operator(select_stmt->filter_stmt());
    if (nullptr == scan_oper) {
      scan_oper = new TableScanOperator(select_stmt->tables()[i]);
    }

    DEFER([&] () {delete scan_oper;});

    PredicateOperator pred_oper(select_stmt->filter_stmt());
    pred_oper.add_child(scan_oper);
    ProjectOperator project_oper;
    project_oper.add_child(&pred_oper);
    for (const Field &field : select_stmt->query_fields()) {  //the query fields in `select` statement
      bool ret = project_oper.add_projection(field.table(), field.meta(),current_scan_table);
      if (ret) {
        const ProjectTuple &project_tuple_ = project_oper.project_tuple();
        const TupleCellSpec *tuple_cell_spec = nullptr;
        project_tuple_.cell_spec_at(project_tuple_.cell_num()-1,tuple_cell_spec);
        const FieldMeta * meta = ((FieldExpr *)tuple_cell_spec->expression())->field().meta();
        int single_offset = meta->offset();
        int multi_field_offset = single_offset + multiple_record_size- trx_len;
        FieldMeta tmp(*meta);
        tmp.set_offset(multi_field_offset);
        tmp.add_table_name(field.table_name());
        multiple_table_project_cell.push_back(tmp);
      }
    }
    multiple_record_size += record_size;

    rc = project_oper.open();
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to open operator");
      return rc;
    }


    //print_tuple_header(ss, project_oper);
    while ((rc = project_oper.next()) == RC::SUCCESS) {
      // get current record
      // write to response
      Tuple * tuple = project_oper.current_tuple();
      if (nullptr == tuple) {
        rc = RC::INTERNAL;
        LOG_WARN("failed to get current record. rc=%s", strrc(rc));
        break;
      }

      //tupleset.add_tuple(*(ProjectTuple *)tuple);
      //int record_size = current_scan_table->table_meta().record_size();
      char* record = new char[record_size];
      multiple_table_record_sizes.push_back(record_size);
      //std::string row;  //make each tupleset(table) ,prepare for descartes
      //store_string_tuple(*tuple, row);
      //tuple_to_string(ss, *tuple);
      //store_tuple(*tuple, record, current_table_trx_length);
      make_full_record(tuple, record, record_size, trx_len);
      tuple_set_.push_back(record);
      
      //ss << std::endl;
    }

    //multi_tuple_set.push_back(std::move(tupleset));

    if (rc != RC::RECORD_EOF) {
      LOG_WARN("something wrong while iterate operator. rc=%s", strrc(rc));
      project_oper.close();
    } else {
      rc = project_oper.close();
    }

    vec.push_back(tuple_set_);

   }
    make_multiple_tuple_header(tuple_header_list, multiple_table_project_cell);
    ss << tuple_header_list << std::endl;
    std::vector<std::string> descartes;
    getDescartes(vec, composite_condition_filter, multiple_table_record_sizes, ss, multiple_table_project_cell);
    
    /*
    for (int i=0; i< descartes.size(); i++) {
      bool first = true;
      for (int j=0; j< multiple_table_fieldmeta.size(); j++) {
        if (!first) {
          ss << " | ";
        }else {
          first = false;
        }
        FieldMeta tmp = multiple_table_fieldmeta[i];
        multi_to_string((char *)descartes[i].c_str(),tmp.offset(),tmp.len(), tmp.type(), ss);
      }
      //descartes[i].c_str();
      ss << std::endl;
    }
    */
   
    session_event->set_response(ss.str());
    return rc;
  }
    //1 if it is an aggregation
    if (select_stmt->is_aggregation()[0] == 1) {
      std::vector<char *> tuple_set_;
      //scan the table and store the data into a vector tuple_set_
      Operator *scan_oper = try_to_create_index_scan_operator(select_stmt->filter_stmt()); 
    if (nullptr == scan_oper) {
      scan_oper = new TableScanOperator(select_stmt->tables()[0]);
    }

    DEFER([&] () {delete scan_oper;});

    const Table *current_scan_table = select_stmt->tables()[0];
    std::stringstream ss;
    PredicateOperator pred_oper(select_stmt->filter_stmt());
    pred_oper.add_child(scan_oper);
    ProjectOperator project_oper;
    project_oper.add_child(&pred_oper);
    for (const Field &field : select_stmt->query_fields()) {  //the query fields in `select` statement
      project_oper.add_projection(field.table(), field.meta());
    }
    rc = project_oper.open();
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to open operator");
      return rc;
    }

    //print_tuple_header(ss, project_oper);
    while ((rc = project_oper.next()) == RC::SUCCESS) {
      // get current record
      // write to response
      Tuple * tuple = project_oper.current_tuple();
      if (nullptr == tuple) {
        rc = RC::INTERNAL;
        LOG_WARN("failed to get current record. rc=%s", strrc(rc));
        break;
      }

      int record_size = current_scan_table->table_meta().record_size();
      char *record = new char[record_size];
      int trx_len = 4;
      make_full_record(tuple, record, record_size, trx_len);
      tuple_set_.push_back(record);
    }

    if (rc != RC::RECORD_EOF) {
      LOG_WARN("something wrong while iterate operator. rc=%s", strrc(rc));
      project_oper.close();
    } else {
      rc = project_oper.close();
    }

    //count the aggr num
    size_t aggr_num = select_stmt->aggr_types().size();
    bool first_header = true;
    //print header
    for (int i = 0; i < aggr_num; i++) {
      if (first_header) {
        first_header = false;
      } else {
        ss << " | ";
      }
      //need aggr type and field name to print header 
      size_t query_num = select_stmt->query_fields().size();
      const Field &field = select_stmt->query_fields()[i];
      AggrType  aggr_type_ = select_stmt->aggr_types()[i];
      char *field_name = nullptr;
      if (aggr_type_ == AggrType::AGGREGATION_COUNT && query_num > 1) {
        field_name = "*";
      } else {
        field_name = (char *)field.field_name();
      }
      
      
      //char * aggr_name = nullptr;
      std::string field_name_('(' + std::string(field_name) + ')');
      std::string aggr_header;
      switch (aggr_type_)
      {
      case AggrType::AGGREGATION_MAX:{
        aggr_header = "MAX" ;
      }break;

      case AggrType::AGGREGATION_MIN:{
        aggr_header = "MIN" ;
      }break;

      case AggrType::AGGREGATION_AVG:{
        aggr_header = "AVG" ;
      }break;

      case AggrType::AGGREGATION_COUNT:{
        aggr_header = "COUNT" ;
      }break;

      default: {
        LOG_WARN("AggrType %d not support!",aggr_type_);
      }break;
      }

      std::string result = aggr_header + field_name_;
      ss << result;

    }
    ss << std::endl;

    bool first_recycle = true;
    for(int i = 0; i < aggr_num; i++) {
      // read the relative field into a vector
      const Field &field = select_stmt->query_fields()[i];
      AggrType first = select_stmt->aggr_types()[i];
      //select_stmt->tables()[0]->table_meta().field_metas()->front().offset();
      size_t trx_length = 4;
      size_t offset = field.meta()->offset() - trx_length;
      size_t length = field.meta()->len();
      AttrType attr_type = field.meta()->type();
      if (first_recycle) {
        first_recycle = false;
      } else {
        ss << " | ";
      }

      switch (first)
      {
      case AGGREGATION_MAX: {
        if(attr_type == AttrType::INTS){
          std::vector<int> array_;
          for (int j = 0; j < tuple_set_.size(); j++) {
            char *tmp = tuple_set_[j];
            array_.push_back(*(int *)(tmp+offset));
        }
        auto max_ = std::max_element(array_.begin(), array_.end());
        ss << *max_;
        } else if(attr_type==AttrType::FLOATS) {
          std::vector<float> array_;
          for (int j = 0; j < tuple_set_.size(); j++) {
            char *tmp = tuple_set_[j];
            array_.push_back(*(float *)(tmp+offset));
          }
        auto max_ = std::max_element(array_.begin(), array_.end());
        ss << *max_;
        } else if(attr_type == AttrType::CHARS){
          //std::sort
          //char *s = nullptr; 
          std::vector<std::string> array_;
          for (int j = 0; j< tuple_set_.size(); j++ ){
            char *tmp = tuple_set_[j]+ offset;
            char *s = new char[length]; 
            for (int k = 0; k < length; k++) {
              if (tmp[k] == '\0') {
                break;
              }
              //s.push_back(tmp[k]);
              s[k] = tmp[k];
            }
            std::cout << s << std::endl;
            std::string str(s);
            std::cout << str << std::endl;
            array_.push_back(s);
            //delete s;
            //s = nullptr;
          }
          std::sort(array_.begin(), array_.end());
          int array_len = array_.size();
          ss << array_.back();  //max
          /*
          // free allocated space
          for (int i = 0; i < array_.size(); i++) {
            delete array_[i];
            array_[i] = nullptr;
          }
          */

        } else {
          LOG_WARN("ATTrType: %d not support!",attr_type);
          rc = RC::GENERIC_ERROR;
        }

      }break;
      case AGGREGATION_MIN: {
        if(attr_type == AttrType::INTS){
          std::vector<int> array_;
          for (int j = 0; j < tuple_set_.size(); j++) {
            char *tmp = tuple_set_[j];
            array_.push_back(*(int *)(tmp+offset));
        }
        auto min_ = std::min_element(array_.begin(), array_.end());
        ss << *min_;
        } else if(attr_type==AttrType::FLOATS) {
          std::vector<float> array_;
          for (int j = 0; j < tuple_set_.size(); j++) {
            char *tmp = tuple_set_[j];
            array_.push_back(*(float *)(tmp+offset));
          }
        auto min_ = std::min_element(array_.begin(), array_.end());
        ss << *min_;

        } else if(attr_type == AttrType::CHARS){
          //std::sort
          
          std::vector<std::string> array_;
          for (int j = 0; j< tuple_set_.size(); j++ ){
            char *tmp = tuple_set_[j]+ offset;
            char *s = new char[length]; 
            for (int k = 0; k < length; k++) {
              if (tmp[k] == '\0') {
                break;
              }
              //s.push_back(tmp[k]);
              s[k] = tmp[k];
            }
            std::cout << s << std::endl;
            std::string str(s);
            std::cout << str << std::endl;
            array_.push_back(s);
            //delete s;
            //s = nullptr;
          }
          std::sort(array_.begin(), array_.end());
          ss << array_[0];

        /*
        // free allocated space
        for (int i = 0; i < array_.size(); i++) {
          delete array_[i];
          array_[i] = nullptr;
        }
        */

        } else {
          LOG_WARN("ATTrType: %d not support!",attr_type);
          rc = RC::GENERIC_ERROR;
        }
        
      }break;
      case AGGREGATION_AVG: {
        
        if(attr_type == AttrType::INTS) {
          //tranform to floats
          std::vector<int> array_;
     
          for (int j = 0; j < tuple_set_.size(); j++) {
            char *tmp = tuple_set_[j];
            array_.push_back(*(int *)(tmp+offset));
          }
    
          float average = accumulate( array_.begin(), array_.end(), 0.0)/array_.size();
          ss << average;
        } else if(attr_type == AttrType::FLOATS) {
          std::vector<float> array_;
          for (int j = 0; j < tuple_set_.size(); j++) {
            char *tmp = tuple_set_[j];
            array_.push_back(*(float *)(tmp+offset));
          }
        
          float average = accumulate( array_.begin(), array_.end(), 0.0)/array_.size();
          ss << average;
        } else {
          LOG_WARN("AttrType: %d not support!",attr_type);
          rc = RC::GENERIC_ERROR;
        }

      }break;
      case AGGREGATION_COUNT: {
        //vector size is the result
        int count_size = tuple_set_.size();
        ss << count_size;
        std::cout << " ss is : " << ss.str()<< std::endl; 
      }break;
    
      default: {
        LOG_WARN("unsupported attr type: %d", AggrType::AGGREGATION_UNDEFINED);
      }break;
      }
    }

    ss << std::endl;
    session_event->set_response(ss.str());
    return rc;
  } 

  Operator *scan_oper = try_to_create_index_scan_operator(select_stmt->filter_stmt());
  if (nullptr == scan_oper) {
    scan_oper = new TableScanOperator(select_stmt->tables()[0]);
  }

  DEFER([&] () {delete scan_oper;});

  PredicateOperator pred_oper(select_stmt->filter_stmt());
  pred_oper.add_child(scan_oper);
  ProjectOperator project_oper;
  project_oper.add_child(&pred_oper);
  for (const Field &field : select_stmt->query_fields()) {  //the query fields in `select` statement
    project_oper.add_projection(field.table(), field.meta());
  }
  rc = project_oper.open();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open operator");
    return rc;
  }

  std::stringstream ss;
  print_tuple_header(ss, project_oper);
  while ((rc = project_oper.next()) == RC::SUCCESS) {
    // get current record
    // write to response
    Tuple * tuple = project_oper.current_tuple();
    if (nullptr == tuple) {
      rc = RC::INTERNAL;
      LOG_WARN("failed to get current record. rc=%s", strrc(rc));
      break;
    }

    tuple_to_string(ss, *tuple);
    ss << std::endl;
  }

  if (rc != RC::RECORD_EOF) {
    LOG_WARN("something wrong while iterate operator. rc=%s", strrc(rc));
    project_oper.close();
  } else {
    rc = project_oper.close();
  }
  session_event->set_response(ss.str());
  return rc;
}


RC ExecuteStage::do_help(SQLStageEvent *sql_event)
{
  SessionEvent *session_event = sql_event->session_event();
  const char *response = "show tables;\n"
                         "desc `table name`;\n"
                         "create table `table name` (`column name` `column type`, ...);\n"
                         "create index `index name` on `table` (`column`);\n"
                         "insert into `table` values(`value1`,`value2`);\n"
                         "update `table` set column=value [where `column`=`value`];\n"
                         "delete from `table` [where `column`=`value`];\n"
                         "select [ * | `columns` ] from `table`;\n";
  session_event->set_response(response);
  return RC::SUCCESS;
}

RC ExecuteStage::do_create_table(SQLStageEvent *sql_event)
{
  const CreateTable &create_table = sql_event->query()->sstr.create_table;
  SessionEvent *session_event = sql_event->session_event();
  Db *db = session_event->session()->get_current_db();
  RC rc = db->create_table(create_table.relation_name,
			create_table.attribute_count, create_table.attributes);
  if (rc == RC::SUCCESS) {
    session_event->set_response("SUCCESS\n");
  } else {
    session_event->set_response("FAILURE\n");
  }
  return rc;
}


RC ExecuteStage::do_drop_table(SQLStageEvent *sql_event)
{
  const DropTable &drop_table = sql_event->query()->sstr.drop_table;
  SessionEvent *session_event = sql_event->session_event();
  Db *db = session_event->session()->get_current_db();
  RC rc = db->drop_table(drop_table.relation_name);
  if (rc == RC::SUCCESS) {
    session_event->set_response("SUCCESS\n");
  } else {
    session_event->set_response("FAILURE\n");
  }
  return rc;
}

RC ExecuteStage::do_create_index(SQLStageEvent *sql_event)
{
  SessionEvent *session_event = sql_event->session_event();
  Db *db = session_event->session()->get_current_db();
  const CreateIndex &create_index = sql_event->query()->sstr.create_index;
  Table *table = db->find_table(create_index.relation_name);
  if (nullptr == table) {
    session_event->set_response("FAILURE\n");
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  RC rc = table->create_index(nullptr, create_index.index_name, create_index.attribute_name);
  sql_event->session_event()->set_response(rc == RC::SUCCESS ? "SUCCESS\n" : "FAILURE\n");
  return rc;
}

RC ExecuteStage::do_show_tables(SQLStageEvent *sql_event)
{
  SessionEvent *session_event = sql_event->session_event();
  Db *db = session_event->session()->get_current_db();
  std::vector<std::string> all_tables;
  db->all_tables(all_tables);
  if (all_tables.empty()) {
    session_event->set_response("No table\n");
  } else {
    std::stringstream ss;
    for (const auto &table : all_tables) {
      ss << table << std::endl;
    }
    session_event->set_response(ss.str().c_str());
  }
  return RC::SUCCESS;
}

RC ExecuteStage::do_desc_table(SQLStageEvent *sql_event)
{
  Query *query = sql_event->query();
  Db *db = sql_event->session_event()->session()->get_current_db();
  const char *table_name = query->sstr.desc_table.relation_name;
  Table *table = db->find_table(table_name);
  std::stringstream ss;
  if (table != nullptr) {
    table->table_meta().desc(ss);
  } else {
    ss << "No such table: " << table_name << std::endl;
  }
  sql_event->session_event()->set_response(ss.str().c_str());
  return RC::SUCCESS;
}

RC ExecuteStage::do_insert(SQLStageEvent *sql_event)
{
  Stmt *stmt = sql_event->stmt();
  SessionEvent *session_event = sql_event->session_event();

  if (stmt == nullptr) {
    LOG_WARN("cannot find statement");
    return RC::GENERIC_ERROR;
  }

  InsertStmt *insert_stmt = (InsertStmt *)stmt;

  Table *table = insert_stmt->table();
  RC rc = table->insert_record(nullptr, insert_stmt->value_amount(), insert_stmt->values());
  if (rc == RC::SUCCESS) {
    session_event->set_response("SUCCESS\n");
  } else {
    session_event->set_response("FAILURE\n");
  }
  return rc;
}

RC ExecuteStage::do_delete(SQLStageEvent *sql_event)
{
  Stmt *stmt = sql_event->stmt();
  SessionEvent *session_event = sql_event->session_event();

  if (stmt == nullptr) {
    LOG_WARN("cannot find statement");
    return RC::GENERIC_ERROR;
  }

  DeleteStmt *delete_stmt = (DeleteStmt *)stmt;
  TableScanOperator scan_oper(delete_stmt->table());
  PredicateOperator pred_oper(delete_stmt->filter_stmt());
  pred_oper.add_child(&scan_oper);
  DeleteOperator delete_oper(delete_stmt);
  delete_oper.add_child(&pred_oper);

  RC rc = delete_oper.open();
  if (rc != RC::SUCCESS) {
    session_event->set_response("FAILURE\n");
  } else {
    session_event->set_response("SUCCESS\n");
  }
  return rc;
}

RC ExecuteStage::do_update(SQLStageEvent *sql_event){
  Stmt *stmt = sql_event->stmt();
  SessionEvent *session_event = sql_event->session_event();

  if (stmt == nullptr) {
    LOG_WARN("cannot find statement");
    return RC::GENERIC_ERROR;
  }

  UpdateStmt *update_stmt = (UpdateStmt *)stmt;

  TableScanOperator scan_oper(update_stmt->table());
  PredicateOperator pred_oper(update_stmt->filter_stmt());
  pred_oper.add_child(&scan_oper);
  UpdateOperator update_oper(update_stmt);
  update_oper.add_child(&pred_oper);

  RC rc = update_oper.open();
  if (rc != RC::SUCCESS) {
    session_event->set_response("FAILURE\n");
  } else {
    session_event->set_response("SUCCESS\n");
  }
  return rc;     
}

    