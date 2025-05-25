#include "include/query_engine/planner/operator/join_physical_operator.h"

JoinPhysicalOperator::JoinPhysicalOperator() = default;

RC JoinPhysicalOperator::open(Trx* trx) {
    if (children_.size() != 2) {
        LOG_WARN("Join operator requires exactly 2 children");
        return RC::INTERNAL;
    }

    trx_ = trx;
    left_empty_ = false;
    right_empty_ = false;
    need_init_ = true; 

    for (auto& child : children_) {
        RC rc = child.get()->open(trx);
        if (rc != RC::SUCCESS) {
            return rc;
        }
    }

    return RC::SUCCESS;
}

RC JoinPhysicalOperator::next() {
    if (children_.size() != 2) {
        return RC::INTERNAL;
    }

    PhysicalOperator* left_child = children_[0].get();
    PhysicalOperator* right_child = children_[1].get();

    if (need_init_) {
        RC rc = left_child->next();
        if (rc != RC::SUCCESS) {
            return rc;
        }
        left_tuple_ = left_child->current_tuple();
        need_init_ = false;
    }

    while (true) {
        RC rc = right_child->next();
        if (rc == RC::SUCCESS) {
            right_tuple_ = right_child->current_tuple();

            joined_tuple_.set_left(left_tuple_);
            joined_tuple_.set_right(right_tuple_);

            Value result;
            rc = condition_->get_value(joined_tuple_, result);
            if (rc != RC::SUCCESS) {
                return rc;
            }

            if (result.get_boolean()) {
                return RC::SUCCESS;
            }
        }
        else if (rc == RC::RECORD_EOF) {
            right_child->close();
            rc = right_child->open(trx_);
            if (rc != RC::SUCCESS) {
                return rc;
            }

            rc = left_child->next();
            if (rc != RC::SUCCESS) {
                return RC::RECORD_EOF;
            }

            left_tuple_ = left_child->current_tuple();
        }
        else {
            return rc;
        }
    }
}

RC JoinPhysicalOperator::close() {
    LOG_DEBUG("Join operator close");
    for (auto& child : children_) {
        child.get()->close();
    }

    return RC::SUCCESS;
}

Tuple* JoinPhysicalOperator::current_tuple() {
    return &joined_tuple_;
}