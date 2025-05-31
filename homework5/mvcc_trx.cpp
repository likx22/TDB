#include "include/storage_engine/transaction/mvcc_trx.h"
#include "include/storage_engine/schema/database.h"

using namespace std;

MvccTrxManager::~MvccTrxManager()
{
  vector<Trx *> tmp_trxes;
  tmp_trxes.swap(trxes_);
  for (Trx *trx : tmp_trxes) {
    delete trx;
  }
}

RC MvccTrxManager::init()
{
  fields_ = vector<FieldMeta>{
      FieldMeta("__trx_xid_begin", AttrType::INTS, 0/*attr_offset*/, 4/*attr_len*/, false/*visible*/),
      FieldMeta("__trx_xid_end",   AttrType::INTS, 4/*attr_offset*/, 4/*attr_len*/, false/*visible*/)
  };
  LOG_INFO("init mvcc trx kit done.");
  return RC::SUCCESS;
}

const vector<FieldMeta> *MvccTrxManager::trx_fields() const
{
  return &fields_;
}

Trx *MvccTrxManager::create_trx(LogManager *log_manager)
{
  Trx *trx = new MvccTrx(*this, log_manager);
  if (trx != nullptr) {
    lock_.lock();
    trxes_.push_back(trx);
    lock_.unlock();
  }
  return trx;
}

Trx *MvccTrxManager::create_trx(int32_t trx_id)
{
  Trx *trx = new MvccTrx(*this, trx_id);
  if (trx != nullptr) {
    lock_.lock();
    trxes_.push_back(trx);
    if (current_trx_id_ < trx_id) {
      current_trx_id_ = trx_id;
    }
    lock_.unlock();
  }
  return trx;
}

void MvccTrxManager::destroy_trx(Trx *trx)
{
  lock_.lock();
  for (auto iter = trxes_.begin(), itend = trxes_.end(); iter != itend; ++iter) {
    if (*iter == trx) {
      trxes_.erase(iter);
      break;
    }
  }
  lock_.unlock();
  delete trx;
}

Trx *MvccTrxManager::find_trx(int32_t trx_id)
{
  lock_.lock();
  for (Trx *trx : trxes_) {
    if (trx->id() == trx_id) {
      lock_.unlock();
      return trx;
    }
  }
  lock_.unlock();
  return nullptr;
}

void MvccTrxManager::all_trxes(std::vector<Trx *> &trxes)
{
  lock_.lock();
  trxes = trxes_;
  lock_.unlock();
}

int32_t MvccTrxManager::next_trx_id()
{
  return ++current_trx_id_;
}

int32_t MvccTrxManager::max_trx_id() const
{
  return numeric_limits<int32_t>::max();
}

void MvccTrxManager::update_trx_id(int32_t trx_id)
{
  int32_t old_trx_id = current_trx_id_;
  while (old_trx_id < trx_id && !current_trx_id_.compare_exchange_weak(old_trx_id, trx_id));
}

////////////////////////////////////////////////////////////////////////////////
MvccTrx::MvccTrx(MvccTrxManager &kit, LogManager *log_manager) : trx_kit_(kit), log_manager_(log_manager)
{}

MvccTrx::MvccTrx(MvccTrxManager &kit, int32_t trx_id) : trx_kit_(kit), trx_id_(trx_id)
{
  started_ = true;
  recovering_ = true;
}

RC MvccTrx::insert_record(Table *table, Record &record)
{
  RC rc = RC::SUCCESS;
  // TODO [Lab4] 需要同学们补充代码实现记录的插入，相关提示见文档

  Field begin_xid_field, end_xid_field;
  trx_fields(table, begin_xid_field, end_xid_field);  // Retrieve the transaction fields for the table

  // Set begin_xid to the negative of the current transaction ID, as per documentation
  begin_xid_field.set_int(record, -trx_id_);
  // Set end_xid to the maximum transaction ID from trx_kit_
  end_xid_field.set_int(record, trx_kit_.max_trx_id());

  // Insert the record into the table
  rc = table->insert_record(record);
  if (rc != RC::SUCCESS) {
    return rc; // Return error code if insertion fails
  }
  

  auto [it, ret] = operations_.emplace(Operation::Type::INSERT, table, record.rid());
  if (!ret) {
    rc = RC::INTERNAL;
    LOG_WARN("failed to insert operation(insertion) into operation set: duplicate");
  }
  // 补充追加日志以供 lab5 redo 使用
  if (!recovering_) {
    log_manager_->append_record_log(
      LogEntryType::INSERT, trx_id_, table->table_id(),
      record.rid(), record.len(), 0, record.data()
    );
  }
  return rc;
}

RC MvccTrx::delete_record(Table *table, Record &record)
{
  RC rc = RC::SUCCESS;
  // TODO [Lab4] 需要同学们补充代码实现逻辑上的删除，相关提示见文档

  Field begin_xid_field, end_xid_field;
  trx_fields(table, begin_xid_field, end_xid_field);  // Retrieve transaction fields for the table
  // According to the documentation, set end_xid to the negative of the current transaction ID for deletion
  int begin_xid = begin_xid_field.get_int(record);
  int end_xid = end_xid_field.get_int(record);

  if (!record_visible(begin_xid, end_xid)) {
    // 不能删除对当前事务不可见的记录
    LOG_ERROR("try to delete an invisible record. begin xid=%d, end xid=%d, trx id=%d", begin_xid, end_xid, trx_id_);
    return RC::INTERNAL;
  }

  if (end_xid != trx_kit_.max_trx_id()) {
    // 不能删除已经被删除的记录
    LOG_ERROR("try to delete a deleted record. begin xid=%d, end xid=%d, trx id=%d", begin_xid, end_xid, trx_id_);
    return RC::LOCKED_CONCURRENCY_CONFLICT;
  }

  rc = delete_record_internal(table, record);
  if (rc != RC::SUCCESS) {
    return rc;
  }
  // 补充追加日志以供 lab5 redo 使用
  if (!recovering_) {
    log_manager_->append_record_log(
      LogEntryType::DELETE, trx_id_, table->table_id(),
      record.rid(), record.len(), 0, record.data()
    );
  }
  return rc;
}

RC MvccTrx::delete_record_internal(Table *table, const Record &record)
{
  RC rc = RC::SUCCESS;

  Field begin_xid_field, end_xid_field;
  trx_fields(table, begin_xid_field, end_xid_field);

  int begin_xid = begin_xid_field.get_int(record);

  if (begin_xid == -trx_id_) {
    // 待删除的记录是当前事务插入的，直接删除
    rc = table->delete_record(record);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    // 撤销插入操作
    int ret = operations_.erase(Operation(Operation::Type::INSERT, table, record.rid()));
    if (ret == 0) {
      LOG_WARN("failed to erase operation(insertion) from operation set: not found");
    }
  } else {
    // 否则，修改记录的删除事务 ID
    rc = table->visit_record(record.rid(), false/*readonly*/, [&](Record &record) {
      end_xid_field.set_int(record, -trx_id_);
    });
    if (rc != RC::SUCCESS) {
      LOG_ERROR("failed to update record end xid while deleting. rc=%s", strrc(rc));
      return rc;
    }
    // 记录删除操作
    auto [it, ret] = operations_.emplace(Operation::Type::DELETE, table, record.rid());
    if (!ret) {
      rc = RC::INTERNAL;
      LOG_WARN("failed to insert operation(deletion) into operation set: duplicate");
    }
  }

  return rc;
}

/**
 * @brief 判断事务 ID 是否对当前事务可见
 * @param xid 事务 ID
 * @return bool 可见性
 */
bool MvccTrx::xid_visible(int xid) const
{
  // 可见条件为：在当前事务中修改，或在当前事务创建前已经发生
  return xid == -trx_id_ || (xid > 0 && xid < trx_id_);
}

/**
 * @brief 判断记录是否对当前事务可见
 * @param begin_xid 记录的创建事务 ID
 * @param end_xid   记录的删除事务 ID
 * @return bool     可见性
 */
bool MvccTrx::record_visible(int begin_xid, int end_xid) const
{
  // 可见条件为：创建操作对当前事务可见，且删除操作对当前事务不可见
  return xid_visible(begin_xid) && !xid_visible(end_xid);
}

/**
   * @brief 当访问到某条数据时，使用此函数来判断是否可见，或者是否有访问冲突
   * @param table    要访问的数据属于哪张表
   * @param record   要访问哪条数据
   * @param readonly 是否只读访问
   * @return RC      - SUCCESS 成功
   *                 - RECORD_INVISIBLE 此数据对当前事务不可见，应该跳过
   *                 - LOCKED_CONCURRENCY_CONFLICT 与其它事务有冲突
 */
RC MvccTrx::visit_record(Table *table, Record &record, bool readonly)
{
  // TODO [Lab4] 需要同学们补充代码实现记录是否可见的判断，相关提示见文档
  Field begin_xid_field, end_xid_field;
  trx_fields(table, begin_xid_field, end_xid_field);  // Retrieve transaction fields
  int begin_xid = begin_xid_field.get_int(record);
  int end_xid = end_xid_field.get_int(record);

  if (!record_visible(begin_xid, end_xid)) {
    // 不能访问对当前事务不可见的记录
    return RC::RECORD_INVISIBLE;
  }

  return RC::SUCCESS;
}

RC MvccTrx::start_if_need()
{
  if (!started_) {
    ASSERT(operations_.empty(), "try to start a new trx while operations is not empty");
    trx_id_ = trx_kit_.next_trx_id();
    LOG_DEBUG("current thread change to new trx with %d", trx_id_);
    RC rc = log_manager_->append_begin_trx_log(trx_id_);
    ASSERT(rc == RC::SUCCESS, "failed to append log to clog. rc=%s", strrc(rc));
    started_ = true;
  }
  return RC::SUCCESS;
}

RC MvccTrx::commit()
{
  int32_t commit_id = trx_kit_.next_trx_id();
  return commit_with_trx_id(commit_id);
}

RC MvccTrx::commit_with_trx_id(int32_t commit_xid)
{
  RC rc = RC::SUCCESS;
  started_ = false;

  if (recovering_) {
    // 在事务恢复时，更新当前事务 id 避免被后续事务重用
    trx_kit_.update_trx_id(commit_xid);
  }

  for (const Operation &operation : operations_) {
    switch (operation.type()) {
      case Operation::Type::INSERT: {
        RID rid(operation.page_num(), operation.slot_num());
        Table *table = operation.table();
        Field begin_xid_field, end_xid_field;
        trx_fields(table, begin_xid_field, end_xid_field);
        auto record_updater = [ this, &begin_xid_field, commit_xid](Record &record) {
          LOG_DEBUG("before commit insert record. trx id=%d, begin xid=%d, commit xid=%d, lbt=%s", trx_id_, begin_xid_field.get_int(record), commit_xid, lbt());
          ASSERT(begin_xid_field.get_int(record) == -this->trx_id_, "got an invalid record while committing. begin xid=%d, this trx id=%d", begin_xid_field.get_int(record), trx_id_);
          begin_xid_field.set_int(record, commit_xid);
        };
        rc = operation.table()->visit_record(rid, false/*readonly*/, record_updater);
        ASSERT(rc == RC::SUCCESS, "failed to get record while committing. rid=%s, rc=%s", rid.to_string().c_str(), strrc(rc));
      } break;

      case Operation::Type::DELETE: {
        Table *table = operation.table();
        RID rid(operation.page_num(), operation.slot_num());
        Field begin_xid_field, end_xid_field;
        trx_fields(table, begin_xid_field, end_xid_field);
        auto record_updater = [this, &end_xid_field, commit_xid](Record &record) {
          (void)this;
          ASSERT(end_xid_field.get_int(record) == -trx_id_, "got an invalid record while committing. end xid=%d, this trx id=%d", end_xid_field.get_int(record), trx_id_);
          end_xid_field.set_int(record, commit_xid);
        };
        rc = operation.table()->visit_record(rid, false/*readonly*/, record_updater);
        ASSERT(rc == RC::SUCCESS, "failed to get record while committing. rid=%s, rc=%s", rid.to_string().c_str(), strrc(rc));
      } break;

      default: {
        ASSERT(false, "unsupported operation. type=%d", static_cast<int>(operation.type()));
      }
    }
  }

  operations_.clear();

  if (!recovering_) {
    rc = log_manager_->append_commit_trx_log(trx_id_, commit_xid);
  }
  LOG_TRACE("append trx commit log. trx id=%d, commit_xid=%d, rc=%s", trx_id_, commit_xid, strrc(rc));

  return rc;
}

RC MvccTrx::rollback()
{
  RC rc = RC::SUCCESS;
  started_ = false;

  for (const Operation &operation : operations_) {
    switch (operation.type()) {
      case Operation::Type::INSERT: {
        RID rid(operation.page_num(), operation.slot_num());
        Record record;
        Table *table = operation.table();
        rc = table->get_record(rid, record);
        ASSERT(rc == RC::SUCCESS, "failed to get record while rollback. rid=%s, rc=%s", rid.to_string().c_str(), strrc(rc));
        rc = table->delete_record(record);
        ASSERT(rc == RC::SUCCESS, "failed to delete record while rollback. rid=%s, rc=%s", rid.to_string().c_str(), strrc(rc));
      } break;

      case Operation::Type::DELETE: {
        Table *table = operation.table();
        RID rid(operation.page_num(), operation.slot_num());
        ASSERT(rc == RC::SUCCESS, "failed to get record while rollback. rid=%s, rc=%s", rid.to_string().c_str(), strrc(rc));
        Field begin_xid_field, end_xid_field;
        trx_fields(table, begin_xid_field, end_xid_field);
        auto record_updater = [this, &end_xid_field](Record &record) {
          ASSERT(end_xid_field.get_int(record) == -trx_id_, "got an invalid record while rollback. end xid=%d, this trx id=%d", end_xid_field.get_int(record), trx_id_);
          end_xid_field.set_int(record, trx_kit_.max_trx_id());
        };
        rc = table->visit_record(rid, false/*readonly*/, record_updater);
        ASSERT(rc == RC::SUCCESS, "failed to get record while committing. rid=%s, rc=%s", rid.to_string().c_str(), strrc(rc));
      } break;

      default: {
        ASSERT(false, "unsupported operation. type=%d", static_cast<int>(operation.type()));
      }
    }
  }

  operations_.clear();

  if (!recovering_) {
    rc = log_manager_->append_rollback_trx_log(trx_id_);
  }
  LOG_TRACE("append trx rollback log. trx id=%d, rc=%s", trx_id_, strrc(rc));
  return rc;
}

/**
 * @brief 获取指定表上的与版本号相关的字段
 * @param table 指定的表
 * @param begin_xid_field 返回处理begin_xid的字段
 * @param end_xid_field   返回处理end_xid的字段
 */
void MvccTrx::trx_fields(Table *table, Field &begin_xid_field, Field &end_xid_field) const
{
  const TableMeta &table_meta = table->table_meta();
  const std::pair<const FieldMeta *, int> trx_fields = table_meta.trx_fields();
  ASSERT(trx_fields.second >= 2, "invalid trx fields number. %d", trx_fields.second);

  begin_xid_field.set_table(table);
  begin_xid_field.set_field(&trx_fields.first[0]);
  end_xid_field.set_table(table);
  end_xid_field.set_field(&trx_fields.first[1]);
}

// TODO [Lab5] 需要同学们补充代码，相关提示见文档
RC MvccTrx::redo(Db *db, const LogEntry &log_entry)
{

  switch (log_entry.log_type()) {
    case LogEntryType::INSERT: {
      const RecordEntry &record_entry = log_entry.record_entry();
      // TODO [Lab5] 需要同学们补充代码，相关提示见文档
      // Find the table corresponding to the table_id in the record entry
      Table* table = db->find_table(record_entry.table_id_);
      if (table == nullptr) {
          // Return error if the table does not exist
          return RC::SCHEMA_TABLE_NOT_EXIST;
      }

      // Need to restore the data record and the Table object it belongs to based on the log entry
      Record record;
      record.set_rid(record_entry.rid_);  // Set the Record ID
      record.set_data(record_entry.data_, record_entry.data_len_);  // Set the data and its length

      // Attempt to recover the inserted record in the table
      RC rc = table->recover_insert_record(record);
      if (rc != RC::SUCCESS) {
          // Return error if the record recovery fails
          return rc;
      }

      operations_.insert(Operation(Operation::Type::INSERT, table, record_entry.rid_));
      break;
    }

    case LogEntryType::DELETE: {
      const RecordEntry &record_entry = log_entry.record_entry();
	  
      // TODO [Lab5] 需要同学们补充代码，相关提示见文档
      // Need to restore the data record and the Table object it belongs to based on the log entry
      Table* table = db->find_table(record_entry.table_id_);
      if (table == nullptr) {
          // Return error if the table does not exist
          return RC::SCHEMA_TABLE_NOT_EXIST;
      }

      // Create a record object and set its record ID and data from the log entry
      Record record;
      record.set_rid(record_entry.rid_);  // Set the Record ID based on the log entry
      record.set_data(record_entry.data_, record_entry.data_len_);  // Set the data and its length from the log entry

      // Attempt to delete the record from the table
      RC rc = delete_record_internal(table, record);
      if (rc != RC::SUCCESS) {
          // Return error if the record deletion fails
          return rc;
      }

      operations_.insert(Operation(Operation::Type::DELETE, table, record_entry.rid_));
      break;
    }

    case LogEntryType::MTR_COMMIT: {
	// TODO [Lab5] 需要同学们补充代码，相关提示见文档
      commit_with_trx_id(log_entry.commit_entry().commit_xid_);
      break;
    }

    case LogEntryType::MTR_ROLLBACK: {

      // TODO [Lab5] 需要同学们补充代码，相关提示见文档
      rollback();
      break;
    }

    default: {
      ASSERT(false, "unsupported redo log. log entry=%s", log_entry.to_string().c_str());
      return RC::INTERNAL;
    } break;
  }

  return RC::SUCCESS;
}