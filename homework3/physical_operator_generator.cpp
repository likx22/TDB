#include "include/query_engine/planner/operator/physical_operator_generator.h"

#include <cmath>
#include <utility>
#include "include/query_engine/planner/node/logical_node.h"
#include "include/query_engine/planner/operator/physical_operator.h"
#include "include/query_engine/planner/node/table_get_logical_node.h"
#include "include/query_engine/planner/operator/table_scan_physical_operator.h"
#include "include/query_engine/planner/node/predicate_logical_node.h"
#include "include/query_engine/planner/operator/predicate_physical_operator.h"
#include "include/query_engine/planner/node/order_by_logical_node.h"
#include "include/query_engine/planner/operator/order_physical_operator.h"
#include "include/query_engine/planner/node/project_logical_node.h"
#include "include/query_engine/planner/operator/project_physical_operator.h"
#include "include/query_engine/planner/node/aggr_logical_node.h"
#include "include/query_engine/planner/operator/aggr_physical_operator.h"
#include "include/query_engine/planner/node/insert_logical_node.h"
#include "include/query_engine/planner/operator/insert_physical_operator.h"
#include "include/query_engine/planner/node/delete_logical_node.h"
#include "include/query_engine/planner/operator/delete_physical_operator.h"
#include "include/query_engine/planner/node/update_logical_node.h"
#include "include/query_engine/planner/operator/update_physical_operator.h"
#include "include/query_engine/planner/node/explain_logical_node.h"
#include "include/query_engine/planner/operator/explain_physical_operator.h"
#include "include/query_engine/planner/node/join_logical_node.h"
#include "include/query_engine/planner/operator/group_by_physical_operator.h"
#include "common/log/log.h"
#include "include/storage_engine/recorder/table.h"
#include "include/query_engine/planner/operator/join_physical_operator.h"
#include "include/query_engine/structor/expression/comparison_expression.h"
#include "include/query_engine/structor/expression/value_expression.h"
#include "include/query_engine/planner/operator/index_scan_physical_operator.h"

using namespace std;

RC PhysicalOperatorGenerator::create(LogicalNode &logical_operator, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  switch (logical_operator.type()) {
    case LogicalNodeType::TABLE_GET: {
      return create_plan(static_cast<TableGetLogicalNode &>(logical_operator), oper, is_delete);
    }

    case LogicalNodeType::PREDICATE: {
      return create_plan(static_cast<PredicateLogicalNode &>(logical_operator), oper, is_delete);
    }

    case LogicalNodeType::ORDER: {
      return create_plan(static_cast<OrderByLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::PROJECTION: {
      return create_plan(static_cast<ProjectLogicalNode &>(logical_operator), oper, is_delete);
    }

    case LogicalNodeType::AGGR: {
      return create_plan(static_cast<AggrLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::INSERT: {
      return create_plan(static_cast<InsertLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::DELETE: {
      return create_plan(static_cast<DeleteLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::UPDATE: {
      return create_plan(static_cast<UpdateLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::EXPLAIN: {
      return create_plan(static_cast<ExplainLogicalNode &>(logical_operator), oper, is_delete);
    }
    // TODO [Lab3] 实现JoinNode到JoinOperator的转换
    case LogicalNodeType::JOIN: {
        return create_plan(static_cast<JoinLogicalNode &>(logical_operator), oper);
    }
    case LogicalNodeType::GROUP_BY: {
      return RC::UNIMPLENMENT;
    }

    default: {
      return RC::INVALID_ARGUMENT;
    }
  }
}

// TODO [Lab2] 
// 在原有的实现中，会直接生成TableScanOperator对所需的数据进行全表扫描，但其实在生成执行计划时，我们可以进行简单的优化：
RC PhysicalOperatorGenerator::create_plan(
    TableGetLogicalNode &table_get_oper, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  vector<unique_ptr<Expression>> &predicates =table_get_oper.predicates();
  Index *index = nullptr;
  
  // 生成IndexScanOperator的准备工作
  const ValueExpr *value_expr = nullptr;
  for (auto &predicate : predicates) {
    if (predicate->type() == ExprType::COMPARISON) {
      auto compare_expr = static_cast<ComparisonExpr *>(predicate.get());
      if (compare_expr->comp() != EQUAL_TO) {
        continue;
      }
      const Expression *left_expr = compare_expr->left().get();
      const Expression *right_expr = compare_expr->right().get();
      if (left_expr->type() == ExprType::FIELD && right_expr->type() == ExprType::VALUE) {
        auto field_expr = static_cast<const FieldExpr *>(left_expr);
        value_expr = static_cast<const ValueExpr *>(right_expr);
        index = table_get_oper.table()->find_index_by_field(field_expr->field_name());
        LOG_DEBUG("find index by field");
        break;
      } 
      else if (left_expr->type() == ExprType::VALUE && right_expr->type() == ExprType::FIELD) {
        auto field_expr = static_cast<const FieldExpr *>(right_expr);
        value_expr = static_cast<const ValueExpr *>(left_expr);
        index = table_get_oper.table()->find_index_by_field(field_expr->field_name());
        break;
      }
      else if (left_expr->type() == ExprType::FIELD && right_expr->type() == ExprType::FIELD) {
        auto left_field_expr = static_cast<const FieldExpr *>(left_expr);
        auto right_field_expr = static_cast<const FieldExpr *>(right_expr);
        index = table_get_oper.table()->find_index_by_field(left_field_expr->field_name());
        if (index == nullptr) {
          index = table_get_oper.table()->find_index_by_field(right_field_expr->field_name());
        }
        break;
      }
    }
  }

  if(index == nullptr){
    LOG_TRACE("use table scan");
    Table *table = table_get_oper.table();
    auto table_scan_oper = new TableScanPhysicalOperator(table, table_get_oper.table_alias(), table_get_oper.readonly());
    table_scan_oper->isdelete_ = is_delete;
    table_scan_oper->set_predicates(std::move(predicates));
    oper = unique_ptr<PhysicalOperator>(table_scan_oper);
  } else {
    LOG_TRACE("use index scan");
    const Value &value = value_expr->get_value();
    auto index_scan_oper = new IndexScanPhysicalOperator(table_get_oper.table(), index, table_get_oper.readonly(), &value, true, &value, true);
    index_scan_oper->isdelete_ = is_delete;
    index_scan_oper->set_predicates(std::move(predicates));
    oper = unique_ptr<PhysicalOperator>(index_scan_oper);
  }

  return RC::SUCCESS;
}

RC PhysicalOperatorGenerator::create_plan(
    PredicateLogicalNode &pred_oper, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  vector<unique_ptr<LogicalNode>> &children_opers = pred_oper.children();
  ASSERT(children_opers.size() == 1, "predicate logical operator's sub oper number should be 1");

  LogicalNode &child_oper = *children_opers.front();

  unique_ptr<PhysicalOperator> child_phy_oper;
  RC rc = create(child_oper, child_phy_oper, is_delete);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create child operator of predicate operator. rc=%s", strrc(rc));
    return rc;
  }

  vector<unique_ptr<Expression>> &expressions = pred_oper.expressions();
  ASSERT(expressions.size() == 1, "predicate logical operator's children should be 1");

  unique_ptr<Expression> expression = std::move(expressions.front());

  oper = unique_ptr<PhysicalOperator>(new PredicatePhysicalOperator(std::move(expression)));
  oper->add_child(std::move(child_phy_oper));
  oper->isdelete_ = is_delete;
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(AggrLogicalNode& aggr_oper, unique_ptr<PhysicalOperator>& oper)
{
    vector<unique_ptr<LogicalNode>>& child_opers = aggr_oper.children();

    unique_ptr<PhysicalOperator> child_phy_oper;

    RC rc = RC::SUCCESS;
    if (!child_opers.empty()) {
        LogicalNode* child_oper = child_opers.front().get();
        rc = create(*child_oper, child_phy_oper);
        if (rc != RC::SUCCESS) {
            LOG_WARN("failed to create project logical operator's child physical operator. rc=%s", strrc(rc));
            return rc;
        }
    }

    auto* aggr_operator = new AggrPhysicalOperator(&aggr_oper);

    if (child_phy_oper) {
        aggr_operator->add_child(std::move(child_phy_oper));
    }

    oper = unique_ptr<PhysicalOperator>(aggr_operator);

    return rc;
}

RC PhysicalOperatorGenerator::create_plan(OrderByLogicalNode& order_oper, unique_ptr<PhysicalOperator>& oper)
{
    vector<unique_ptr<LogicalNode>>& child_opers = order_oper.children();

    unique_ptr<PhysicalOperator> child_phy_oper;

    RC rc = RC::SUCCESS;
    if (!child_opers.empty()) {
        LogicalNode* child_oper = child_opers.front().get();
        rc = create(*child_oper, child_phy_oper);
        if (rc != RC::SUCCESS) {
            LOG_WARN("failed to create project logical operator's child physical operator. rc=%s", strrc(rc));
            return rc;
        }
    }

    OrderPhysicalOperator* order_operator = new OrderPhysicalOperator(std::move(order_oper.order_units()));

    if (child_phy_oper) {
        order_operator->add_child(std::move(child_phy_oper));
    }

    oper = unique_ptr<PhysicalOperator>(order_operator);

    return rc;
}

RC PhysicalOperatorGenerator::create_plan(
    ProjectLogicalNode& project_oper, unique_ptr<PhysicalOperator>& oper, bool is_delete)
{
    vector<unique_ptr<LogicalNode>>& child_opers = project_oper.children();

    unique_ptr<PhysicalOperator> child_phy_oper;

    RC rc = RC::SUCCESS;
    if (!child_opers.empty()) {
        LogicalNode* child_oper = child_opers.front().get();
        rc = create(*child_oper, child_phy_oper, is_delete);
        if (rc != RC::SUCCESS) {
            LOG_WARN("failed to create project logical operator's child physical operator. rc=%s", strrc(rc));
            return rc;
        }
    }

    auto* project_operator = new ProjectPhysicalOperator(&project_oper);
    for (const auto& i : project_oper.expressions()) {
        project_operator->add_projector(i->copy());
    }

    if (child_phy_oper) {
        project_operator->add_child(std::move(child_phy_oper));
    }

    oper = unique_ptr<PhysicalOperator>(project_operator);
    oper->isdelete_ = is_delete;

    return rc;
}

RC PhysicalOperatorGenerator::create_plan(InsertLogicalNode& insert_oper, unique_ptr<PhysicalOperator>& oper)
{
    Table* table = insert_oper.table();
    vector<vector<Value>> multi_values;
    for (int i = 0; i < insert_oper.multi_values().size(); i++) {
        vector<Value>& values = insert_oper.values(i);
        multi_values.push_back(values);
    }
    InsertPhysicalOperator* insert_phy_oper = new InsertPhysicalOperator(table, std::move(multi_values));
    oper.reset(insert_phy_oper);
    return RC::SUCCESS;
}

RC PhysicalOperatorGenerator::create_plan(UpdateLogicalNode& update_oper, unique_ptr<PhysicalOperator>& oper)
{
    vector<unique_ptr<LogicalNode>>& child_opers = update_oper.children();

    unique_ptr<PhysicalOperator> child_physical_oper;

    RC rc = RC::SUCCESS;
    if (!child_opers.empty()) {
        LogicalNode* child_oper = child_opers.front().get();
        rc = create(*child_oper, child_physical_oper);
        if (rc != RC::SUCCESS) {
            LOG_WARN("failed to create physical operator. rc=%s", strrc(rc));
            return rc;
        }
    }

    oper = unique_ptr<PhysicalOperator>(new UpdatePhysicalOperator(update_oper.table(), update_oper.update_units()));

    if (child_physical_oper) {
        oper->add_child(std::move(child_physical_oper));
    }
    return rc;
}

RC PhysicalOperatorGenerator::create_plan(
    ExplainLogicalNode& explain_oper, unique_ptr<PhysicalOperator>& oper, bool is_delete)
{
    vector<unique_ptr<LogicalNode>>& child_opers = explain_oper.children();

    RC rc = RC::SUCCESS;
    unique_ptr<PhysicalOperator> explain_physical_oper(new ExplainPhysicalOperator);
    for (unique_ptr<LogicalNode>& child_oper : child_opers) {
        unique_ptr<PhysicalOperator> child_physical_oper;
        rc = create(*child_oper, child_physical_oper, is_delete);
        if (rc != RC::SUCCESS) {
            LOG_WARN("failed to create child physical operator. rc=%s", strrc(rc));
            return rc;
        }

        explain_physical_oper->add_child(std::move(child_physical_oper));
    }

    oper = std::move(explain_physical_oper);
    oper->isdelete_ = is_delete;
    return rc;
}

RC PhysicalOperatorGenerator::create_plan(DeleteLogicalNode& delete_oper, unique_ptr<PhysicalOperator>& oper)
{
    vector<unique_ptr<LogicalNode>>& child_opers = delete_oper.children();

    unique_ptr<PhysicalOperator> child_physical_oper;

    RC rc = RC::SUCCESS;
    if (!child_opers.empty()) {
        LogicalNode* child_oper = child_opers.front().get();
        rc = create(*child_oper, child_physical_oper, true);
        if (rc != RC::SUCCESS) {
            LOG_WARN("failed to create physical operator. rc=%s", strrc(rc));
            return rc;
        }
    }

    oper = unique_ptr<PhysicalOperator>(new DeletePhysicalOperator(delete_oper.table()));
    oper->isdelete_ = true;
    if (child_physical_oper) {
        oper->add_child(std::move(child_physical_oper));
    }
    return rc;
}

// TODO [Lab3] 根据LogicalNode生成对应的PhyiscalOperator
RC PhysicalOperatorGenerator::create_plan(JoinLogicalNode& join_oper, unique_ptr<PhysicalOperator>& oper)
{
    LOG_DEBUG("create join physical operator");
    vector<unique_ptr<LogicalNode>>& child_opers = join_oper.children();

    unique_ptr<PhysicalOperator> left_child_physical_oper;
    unique_ptr<PhysicalOperator> right_child_physical_oper;

    RC rc = RC::SUCCESS;
    if (!child_opers.empty()) {
        LogicalNode* left_child_oper = child_opers[0].get();
        LogicalNode* right_child_oper = child_opers[1].get();
        rc = create(*left_child_oper, left_child_physical_oper);
        if (rc != RC::SUCCESS) {
            LOG_WARN("failed to create join logical operator's left physical operator. rc=%s", strrc(rc));
            return rc;
        }
        rc = create(*right_child_oper, right_child_physical_oper);
        if (rc != RC::SUCCESS) {
            LOG_WARN("failed to create join logical operator's right physical operator. rc=%s", strrc(rc));
            return rc;
        }
    }
    std::unique_ptr<Expression>& condition = join_oper.condition();
    oper = unique_ptr<PhysicalOperator>(new JoinPhysicalOperator(std::move(condition)));
    oper->add_child(std::move(left_child_physical_oper));
    oper->add_child(std::move(right_child_physical_oper));

    return rc;
}