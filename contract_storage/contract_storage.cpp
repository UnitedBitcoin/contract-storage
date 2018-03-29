#include <contract_storage/contract_storage.hpp>
#include <contract_storage/config.hpp>
#include <contract_storage/exceptions.hpp>
#include <fc/io/json.hpp>
#include <fc/string.hpp>
#include <fc/crypto/base64.hpp>
#include <boost/scope_exit.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>

namespace contract
{
	namespace storage
	{
		using namespace jsondiff;

		static const std::string root_state_hash_key = "ROOT_STATE_HASH";
		static const std::string top_root_state_hash_key = "TOP_ROOT_STATE_HASH";

		static std::string make_contract_info_key(const std::string& contract_id)
		{
			return std::string("contract_info_key_") + contract_id;
		}

		static std::string make_contract_storage_key(const std::string& contract_id, const std::string &storage_name)
		{
			return std::string("contract_storage_key_") + contract_id + "_" + storage_name;
		}

		static std::string make_event_id_prefix(const ContractCommitId& commit_id) {
			return std::string("event$") + commit_id;
		}

		static std::string make_event_id(const ContractCommitId& commit_id, size_t index_in_commit)
		{
			return make_event_id_prefix(commit_id) + std::to_string(index_in_commit);
		}

		static std::string make_commit_events_key(const ContractCommitId& commit_id) {
			return std::string("commit_events$") + commit_id;
		}

		static std::string make_commit_event_key_prefix_of_commit(const ContractCommitId& commit_id) {
			return std::string("commit_event$") + commit_id + "$";
		}

		static std::string make_commit_event_key(const ContractCommitId& commit_id, const std::string& event_id)
		{
			return make_commit_event_key_prefix_of_commit(commit_id) + event_id;
		}

		static std::string make_transaction_event_key_prefix_of_transaction_id(const std::string& transaction_id) {
			if (transaction_id.empty())
				BOOST_THROW_EXCEPTION(ContractStorageException("empty transaction id error"));
			return std::string("transaction_event$") + transaction_id + "$";
		}

		static std::string make_transaction_event_key(const std::string& transaction_id, const std::string& event_id)
		{
			return make_transaction_event_key_prefix_of_transaction_id(transaction_id) + event_id;
		}

		static std::string make_transaction_events_key(const std::string& transaction_id) {
			return std::string("transaction_events$") + transaction_id;
		}

		static std::string make_contract_name_id_mapping_key(const std::string& contract_name)
		{
			return std::string("contract_name_id_mapping_") + contract_name;
		}

		ContractStorageService::ContractStorageService(uint32_t magic_number, const std::string& storage_db_path, const std::string& storage_sql_db_path)
			: _db(nullptr), _sql_db(nullptr), _magic_number(magic_number), _storage_db_path(storage_db_path), _storage_sql_db_path(storage_sql_db_path)
		{
			open();
		}
		ContractStorageService::~ContractStorageService()
		{
			close();
		}

		void ContractStorageService::open()
		{
			if (!_db)
			{
				leveldb::Options options;
				options.create_if_missing = true;
				auto status = leveldb::DB::Open(options, _storage_db_path, &_db);
				assert(status.ok());
			}
			if (!_sql_db)
			{
				auto status = sqlite3_open(_storage_sql_db_path.c_str(), &_sql_db);
				assert(status == SQLITE_OK);
				// init tables
				this->init_commits_table();
			}
		}

		void ContractStorageService::close()
		{
			if (_db)
			{
				delete _db;
				_db = nullptr;
			}
			if (_sql_db)
			{
				sqlite3_close(_sql_db);
				_sql_db = nullptr;
			}
		}

		static int empty_sql_callback(void *notUsed, int argc, char **argv, char **colNames)
		{
			return 0;
		}

		void ContractStorageService::init_commits_table()
		{
			char *errMsg;
			// TODO: auto generate bigint id auto increment, maybe need use a new config table. or use GUID as id
			auto status = sqlite3_exec(_sql_db, "CREATE TABLE IF NOT EXISTS commit_info (id INTEGER PRIMARY KEY, commit_id varchar(255) not null, change_type varchar(50) not null, contract_id varchar(255))",
				&empty_sql_callback, nullptr, &errMsg);
			if (status != SQLITE_OK)
			{
				sqlite3_free(errMsg);
				BOOST_THROW_EXCEPTION(ContractStorageException(errMsg));
			}
		}

		static int query_records_sql_callback(void *json_array_ptr, int argc, char **argv, char **colNames)
		{
			auto json_array = (jsondiff::JsonArray*) json_array_ptr;
			jsondiff::JsonObject record;
			int i;
			for (i = 0; i < argc; i++) {
				auto colName = colNames[i];
				if (argv[i])
					record[colName] = argv[i];
				else
					record[colName] = jsondiff::JsonValue();
			}
			json_array->push_back(record);
			return 0;
		}

		ContractCommitInfoP ContractStorageService::get_commit_info(const ContractCommitId& commit_id)
		{
			check_db();
			char *errMsg;
			jsondiff::JsonArray records;
			auto query_sql = std::string("select id, commit_id, change_type, contract_id from commit_info where commit_id='") + commit_id + "'";
			auto status = sqlite3_exec(_sql_db, query_sql.c_str(),
				&query_records_sql_callback, &records, &errMsg);
			if (status != SQLITE_OK)
			{
				std::string err_msg_str(errMsg);
				sqlite3_free(errMsg);
				BOOST_THROW_EXCEPTION(ContractStorageException(err_msg_str));
			}
			if (records.empty())
				return nullptr;
			jsondiff::JsonObject found_record = records[0].as<jsondiff::JsonObject>();
			auto commit_info = std::make_shared<ContractCommitInfo>();
			commit_info->id = found_record["id"].as_uint64();
			commit_info->commit_id = found_record["commit_id"].as_string();
			commit_info->contract_id = found_record["contract_id"].as_string();
			commit_info->change_type = found_record["change_type"].as_string();
			return commit_info;
		}

		void ContractStorageService::add_commit_info(ContractCommitId commit_id, const std::string &change_type, const std::string &diff_str, const std::string &contract_id)
		{
			check_db();
			auto commit_info_existed = get_commit_info(commit_id);
			if (commit_info_existed)
			{
				BOOST_THROW_EXCEPTION(ContractStorageException("same commitId existed before"));
			}
			char *insert_err;
			auto insert_sql = std::string("insert into commit_info (commit_id, change_type, contract_id) values ('") + commit_id + "','" + change_type + "', '" + contract_id + "')";
			auto insert_status = sqlite3_exec(_sql_db,
				insert_sql.c_str(), &empty_sql_callback, nullptr, &insert_err);
			if (insert_status != SQLITE_OK)
			{
				sqlite3_free(insert_err);
				BOOST_THROW_EXCEPTION(ContractStorageException("insert contract change commit to db error"));
			}
			leveldb::WriteOptions write_options;
			auto save_diff_status = _db->Put(write_options, commit_id, diff_str);
			if (!save_diff_status.ok())
				BOOST_THROW_EXCEPTION(ContractStorageException("save contract info diff to db error"));
		}

		std::string ContractStorageService::get_value_by_key_or_error(const std::string &key)
		{
			check_db();
			leveldb::ReadOptions read_options;
			std::string value;
			auto status = _db->Get(read_options, key, &value);
			if (!status.ok())
				BOOST_THROW_EXCEPTION(ContractStorageException(std::string("Can't find value by key ") + key));
			return value;
		}

		jsondiff::JsonValue ContractStorageService::get_json_value_by_key_or_null(const std::string &key)
		{
			check_db();
			leveldb::ReadOptions read_options;
			std::string value;
			auto status = _db->Get(read_options, key, &value);
			if (!status.ok())
				return jsondiff::JsonValue();
			return jsondiff::json_loads(value);
		}

		fc::sha256 ContractStorageService::hash_new_contract_info_commit(ContractInfoP contract_info) const
		{
			return ordered_json_digest(contract_info->to_json());
		}

		fc::sha256 ContractStorageService::hash_contract_changes(ContractChangesP changes) const
		{
			return ordered_json_digest(changes->to_json());
		}

		void ContractStorageService::check_db() const
		{
			if (!_db)
				BOOST_THROW_EXCEPTION(ContractStorageException("contract storage db not opened"));
			if (!_sql_db)
				BOOST_THROW_EXCEPTION(ContractStorageException("contract storage sql db not opened"));
		}

		void ContractStorageService::begin_sql_transaction()
		{
			check_db();
			char *err;
			if (sqlite3_exec(_sql_db, "BEGIN", nullptr, nullptr, &err) != SQLITE_OK)
			{
				std::string err_str = std::string("contract sql transaction begin error ") + err;
				sqlite3_free(err);
				BOOST_THROW_EXCEPTION(ContractStorageException(err_str));
			}
		}
		void ContractStorageService::commit_sql_transaction()
		{
			check_db();
			char *err;
			if (sqlite3_exec(_sql_db, "COMMIT", nullptr, nullptr, &err) != SQLITE_OK)
			{
				std::string err_str = std::string("contract sql transaction commit error ") + err;
				sqlite3_free(err);
				BOOST_THROW_EXCEPTION(ContractStorageException(err_str));
			}
		}
		void ContractStorageService::rollback_sql_transaction()
		{
			check_db();
			char *err;
			if (sqlite3_exec(_sql_db, "ROLLBACK", nullptr, nullptr, &err) != SQLITE_OK)
			{
				std::string err_str = std::string("contract sql transaction rollback error ") + err;
				sqlite3_free(err);
				BOOST_THROW_EXCEPTION(ContractStorageException(err_str));
			}
		}

		void ContractStorageService::rollback_leveldb_transaction(const leveldb::Snapshot* snapshot_to_rollback, const std::vector<std::string>& changed_keys)
		{
			// leveldb rollback using snapshot
			leveldb::WriteOptions write_options;
			leveldb::ReadOptions read_options_for_snapshot;
			read_options_for_snapshot.snapshot = snapshot_to_rollback;
			for (const auto& key : changed_keys) {
				std::string old_value;
				auto status = _db->Get(read_options_for_snapshot, key, &old_value);
				if (status.ok())
				{
					_db->Put(write_options, key, old_value);
				}
				else if (status.IsNotFound())
				{
					_db->Delete(write_options, key);
				}
			}
		}

		ContractInfoP ContractStorageService::get_contract_info(const AddressType& contract_id) const
		{
			check_db();
			leveldb::ReadOptions options;
			std::string value;
			auto status = _db->Get(options, make_contract_info_key(contract_id), &value);
			if (!status.ok()) {
				return nullptr;
			}
			auto json_value = jsondiff::json_loads(value);
			if (!json_value.is_object())
				BOOST_THROW_EXCEPTION(ContractStorageException("contract info db data error"));
			auto json_obj = json_value.as<jsondiff::JsonObject>();
			auto contract_info = ContractInfo::from_json(json_obj);
			return contract_info;
		}

		AddressType ContractStorageService::find_contract_id_by_name(const std::string& name) const
		{
			check_db();
			leveldb::ReadOptions options;
			std::string contract_id;
			auto status = _db->Get(options, make_contract_name_id_mapping_key(name), &contract_id);
			if (!status.ok())
			{
				return "";
			}
			auto contract_info = get_contract_info(contract_id);
			if (contract_info)
				return contract_id;
			else
				return "";
		}

		std::string ContractStorageService::current_root_state_hash() const
		{
			check_db();
			leveldb::ReadOptions read_options;
			std::string state_hash;
			if (!_db->Get(read_options, root_state_hash_key, &state_hash).ok())
				state_hash = EMPTY_COMMIT_ID;
			return state_hash;
		}

		ContractCommitId ContractStorageService::top_root_state_hash() const
		{
			check_db();
			leveldb::ReadOptions read_options;
			std::string state_hash;
			if (!_db->Get(read_options, top_root_state_hash_key, &state_hash).ok())
				state_hash = EMPTY_COMMIT_ID;
			return state_hash;
		}

		// TODO: only use leveldb and use leveldb transaction

		ContractCommitId ContractStorageService::save_contract_info(ContractInfoP contract_info)
		{
			check_db();
			bool success = false;
			leveldb::WriteOptions write_options;
			// snapshot leveldb for rollback
			const auto snapshot = _db->GetSnapshot();
			BOOST_SCOPE_EXIT_ALL(&) {
				_db->ReleaseSnapshot(snapshot);
			};
			std::vector<std::string> changed_leveldb_keys;
			begin_sql_transaction();
			BOOST_SCOPE_EXIT_ALL(&) {
				if (success)
				{
					commit_sql_transaction();
				}
				else
				{
					rollback_sql_transaction();
					rollback_leveldb_transaction(snapshot, changed_leveldb_keys);
				}
			};
			const auto& old_root_state_hash = current_root_state_hash();
			const auto& top_commit_id = top_root_state_hash();
			if (old_root_state_hash != top_commit_id) {
				rollback_to_root_state_hash_without_transactional(old_root_state_hash, changed_leveldb_keys);
				assert(current_root_state_hash() == old_root_state_hash);
			}

			auto key = make_contract_info_key(contract_info->id);
			leveldb::ReadOptions read_options;
			std::string old_value;
			jsondiff::JsonObject old_json_value;
			auto read_status = _db->Get(read_options, key, &old_value);
			if (read_status.ok())
			{
				old_json_value = jsondiff::json_loads(old_value).as<jsondiff::JsonObject>();
			}

			auto json_obj = contract_info->to_json();
			auto status = _db->Put(write_options, key, jsondiff::json_dumps(json_obj));
			if (!status.ok())
				BOOST_THROW_EXCEPTION(ContractStorageException("save contract info to db error"));
			changed_leveldb_keys.push_back(key);
			jsondiff::JsonDiff differ;
			auto contract_info_diff = differ.diff(old_json_value, json_obj);
			std::string contract_info_diff_str = contract_info_diff->str();

			// add mapping of contract_name => contract_id
			if (contract_info->name.size() > 0)
			{
				// check name unique(exist contract with this name's id must be same or empty)
				const auto& contract_name_id_mapping_key = make_contract_name_id_mapping_key(contract_info->name);
				std::string exist_name_id;
				if (_db->Get(read_options, contract_name_id_mapping_key, &exist_name_id).ok() && exist_name_id != contract_info->id)
					BOOST_THROW_EXCEPTION(ContractStorageException(std::string("contract name ") + contract_info->name + " existed before"));
				if (!_db->Put(write_options, contract_name_id_mapping_key, contract_info->id).ok())
					BOOST_THROW_EXCEPTION(ContractStorageException("save contract name => contract id mapping to db error"));
				changed_leveldb_keys.push_back(contract_name_id_mapping_key);
			}

			// update root-state-hash
			const auto& root_state_hash = generate_next_root_hash(old_root_state_hash, hash_new_contract_info_commit(contract_info));
			ContractCommitId commitId = root_state_hash;
			add_commit_info(commitId, CONTRACT_INFO_CHANGE_TYPE, contract_info_diff_str, contract_info->id);
			if (!_db->Put(write_options, root_state_hash_key, root_state_hash).ok())
				BOOST_THROW_EXCEPTION(ContractStorageException("update root state hash error"));
			changed_leveldb_keys.push_back(root_state_hash_key);
			if (!_db->Put(write_options, top_root_state_hash_key, root_state_hash).ok())
				BOOST_THROW_EXCEPTION(ContractStorageException("update top root state hash error"));
			changed_leveldb_keys.push_back(top_root_state_hash_key);
			success = true;
			return commitId;
		}

		bool ContractStorageService::is_latest() const
		{
			check_db();
			const auto& old_root_state_hash = current_root_state_hash();
			const auto& top_commit_id = top_root_state_hash();
			return old_root_state_hash == top_commit_id;
		}

		jsondiff::JsonValue ContractStorageService::get_contract_storage(AddressType contract_id, const std::string& storage_name) const
		{
			check_db();
			auto key = make_contract_storage_key(contract_id, storage_name);
			auto snapshot = _db->GetSnapshot();
			BOOST_SCOPE_EXIT_ALL(&) {
				_db->ReleaseSnapshot(snapshot);
			};
			leveldb::ReadOptions options;
			options.snapshot = snapshot;
			std::string value;
			auto status = _db->Get(options, key, &value);
			if (!status.ok())
				return jsondiff::JsonValue();
			return jsondiff::json_loads(value);
		}
		std::vector<ContractBalance> ContractStorageService::get_contract_balances(const AddressType& contract_id) const
		{
			check_db();
			auto snapshot = _db->GetSnapshot();
			BOOST_SCOPE_EXIT_ALL(&) {
				_db->ReleaseSnapshot(snapshot);
			};
			leveldb::ReadOptions options;
			options.snapshot = snapshot;
			std::string value;
			std::vector<ContractBalance> result;
			auto status = _db->Get(options, make_contract_info_key(contract_id), &value);
			if (!status.ok()) {
				return result;
			}
			auto json_value = jsondiff::json_loads(value);
			if (!json_value.is_object())
				BOOST_THROW_EXCEPTION(ContractStorageException("contract info db data error"));
			auto contract_info = std::make_shared<ContractInfo>();
			auto json_obj = json_value.as<jsondiff::JsonObject>();
			auto balances_json_array = json_obj["balances"].as<jsondiff::JsonArray>();
			for (size_t i = 0; i < balances_json_array.size(); i++)
			{
				auto balance_item_json = balances_json_array[i].as<jsondiff::JsonObject>();
				ContractBalance balance;
				balance.asset_id = (uint32_t) balance_item_json["asset_id"].as_uint64();
				balance.amount = balance_item_json["amount"].as_uint64();
				result.push_back(balance);
			}
			return result;
		}

		std::shared_ptr<std::vector<ContractEventInfo>> ContractStorageService::get_commit_events(const ContractCommitId& commit_id) const
		{
			check_db();
			auto events = std::make_shared<std::vector<ContractEventInfo>>();
			auto snapshot = _db->GetSnapshot();
			BOOST_SCOPE_EXIT_ALL(&) {
				_db->ReleaseSnapshot(snapshot);
			};
			leveldb::ReadOptions read_options;
			read_options.snapshot = snapshot;
			const auto& commit_events_key = make_commit_events_key(commit_id);

			std::string events_str_value;
			if (_db->Get(read_options, commit_events_key, &events_str_value).ok()) {
				const auto& json_obj = jsondiff::json_loads(events_str_value);
				if (json_obj.is_array()) {
					*events = ContractChanges::events_from_json(json_obj.as<jsondiff::JsonArray>());
				}
			}
			return events; 
		}

		std::shared_ptr<std::vector<ContractEventInfo>> ContractStorageService::get_transaction_events(const std::string& transaction_id) const
		{
			check_db();
			auto events = std::make_shared<std::vector<ContractEventInfo>>();
			auto snapshot = _db->GetSnapshot();
			BOOST_SCOPE_EXIT_ALL(&) {
				_db->ReleaseSnapshot(snapshot);
			};
			leveldb::ReadOptions read_options;
			read_options.snapshot = snapshot;

			const auto& tx_events_key = make_transaction_events_key(transaction_id);
			std::string value;
			if (_db->Get(read_options, tx_events_key, &value).ok()) {
				const auto& events_json = jsondiff::json_loads(value);
				if (events_json.is_array()) {
					*events = ContractChanges::events_from_json(events_json.as<jsondiff::JsonArray>());
				}
			}
			return events;
		}

		void ContractStorageService::clear_sql_db()
		{
			check_db();
			char *err_msg;
			auto drop_status = sqlite3_exec(_sql_db, "delete from commit_info", &empty_sql_callback, nullptr, &err_msg);
			if (drop_status != SQLITE_OK)
			{
				std::string err_msg_str(err_msg);
				sqlite3_free(err_msg);
				BOOST_THROW_EXCEPTION(ContractStorageException(err_msg_str));
			}
		}

		// not support concurrency now
		// TODO: add lock
		// save commit history with all diffs
		ContractCommitId ContractStorageService::commit_contract_changes(ContractChangesP changes)
		{
			check_db();
			leveldb::ReadOptions read_options;
			leveldb::WriteOptions write_options;
			// snapshot leveldb for rollback
			const auto snapshot = _db->GetSnapshot();
			BOOST_SCOPE_EXIT_ALL(&) {
				_db->ReleaseSnapshot(snapshot);
			};
			std::vector<std::string> changed_leveldb_keys;
			const auto& old_root_state_hash = current_root_state_hash();
			const auto& top_commit_id = top_root_state_hash();
			if (old_root_state_hash != top_commit_id) {
				rollback_to_root_state_hash_without_transactional(old_root_state_hash, changed_leveldb_keys);
				assert(current_root_state_hash() == old_root_state_hash);
			}
			const auto& root_state_hash = generate_next_root_hash(old_root_state_hash, hash_contract_changes(changes));
			ContractCommitId commitId = root_state_hash;
			// check commitId not conflict
			if(get_commit_info(commitId))
				BOOST_THROW_EXCEPTION(ContractStorageException("same commitId existed before"));
			bool success = false;
			begin_sql_transaction();
			BOOST_SCOPE_EXIT_ALL(&) {
				if (success)
				{
					commit_sql_transaction();
				}
				else
				{
					rollback_sql_transaction();
					rollback_leveldb_transaction(snapshot, changed_leveldb_keys);
				}
			};
			// merge change to leveldb
			for (const auto &balance_change : changes->balance_changes)
			{
				if (!balance_change.is_contract)
					continue;
				auto balances = get_contract_balances(balance_change.address);
				auto found_balance = false;
				for (auto &balance : balances)
				{
					if (balance.asset_id == balance_change.asset_id)
					{
						found_balance = true;
						if (!balance_change.add && (balance.amount < balance_change.amount)) {
							BOOST_THROW_EXCEPTION(ContractStorageException("contract balance can't be negative"));
						}
						balance.amount = balance_change.add ? (balance.amount + balance_change.amount) : (balance.amount - balance_change.amount);
						break;
					}
				}
				if (!found_balance)
				{
					ContractBalance balance;
					balance.amount = balance_change.add ? balance_change.amount : 0;
					balance.asset_id = balance_change.asset_id;
					balances.push_back(balance);
				}
				std::string value;
				auto contract_info_key = make_contract_info_key(balance_change.address);
				auto status = _db->Get(read_options, contract_info_key, &value);
				if (!status.ok()) {
					BOOST_THROW_EXCEPTION(ContractStorageException("contract info not found to transfer balance"));
				}
				auto json_value = jsondiff::json_loads(value);
				if (!json_value.is_object())
					BOOST_THROW_EXCEPTION(ContractStorageException("contract info db data error"));
				auto json_obj = json_value.as<jsondiff::JsonObject>();
				jsondiff::JsonArray balances_json_array;
				for (const auto &balance : balances)
				{
					balances_json_array.push_back(balance.to_json());
				}
				json_obj["balances"] = balances_json_array;
				const auto& new_contract_info_value = jsondiff::json_dumps(json_obj);
				auto write_status = _db->Put(write_options, contract_info_key, new_contract_info_value);
				if(!write_status.ok())
					BOOST_THROW_EXCEPTION(ContractStorageException("contract info write to db error"));
				changed_leveldb_keys.push_back(contract_info_key);
			}
			jsondiff::JsonDiff differ;
			for (const auto &storage_change : changes->storage_changes)
			{
				const auto &contract_id = storage_change.contract_id;
				for (const auto &storage_change_item : storage_change.items)
				{
					const auto& storage_old_value = get_contract_storage(contract_id, storage_change_item.name);
					const auto& storage_value = differ.patch(storage_old_value, storage_change_item.diff);
					const auto& key = make_contract_storage_key(contract_id, storage_change_item.name);
					auto status = _db->Put(write_options, key, jsondiff::json_dumps(storage_value));
					if (!status.ok())
						BOOST_THROW_EXCEPTION(ContractStorageException("contract storage write to db error"));
					changed_leveldb_keys.push_back(key);
				}
			}

			// events save
			auto transaction_events = std::make_shared<std::map<std::string, std::vector<ContractEventInfo>>>();
			for (size_t i=0;i<changes->events.size();i++)
			{
				const auto& event_info = changes->events[i];
				// let event_id => "event" + commit_id + index
				// save {event_id} => event json info, event_commit_{commit_id}_{event_id} => event_id, event_transaction_{transaction_id}_{event_id} => event_id if transaction id not empty
				const auto& event_id = make_event_id(commitId, i);
				const auto& event_json = event_info.to_json();
				if (!_db->Put(write_options, event_id, jsondiff::json_dumps(event_json)).ok()) {
					BOOST_THROW_EXCEPTION(ContractStorageException("event info save error"));
				}
				changed_leveldb_keys.push_back(event_id);
				const auto& commit_event_key = make_commit_event_key(commitId, event_id);
				if (!_db->Put(write_options, commit_event_key, event_id).ok()) {
					BOOST_THROW_EXCEPTION(ContractStorageException("event info save error"));
				}
				changed_leveldb_keys.push_back(commit_event_key);
				if (!event_info.transaction_id.empty()) {
					const auto& transaction_event_key = make_transaction_event_key(event_info.transaction_id, event_id);
					if (!_db->Put(write_options, transaction_event_key, event_id).ok()) {
						BOOST_THROW_EXCEPTION(ContractStorageException("event info save error"));
					}
					changed_leveldb_keys.push_back(transaction_event_key);
					if (transaction_events->find(event_info.transaction_id) == transaction_events->end()) {
						(*transaction_events)[event_info.transaction_id] = std::vector<ContractEventInfo>();
					}
					(*transaction_events)[event_info.transaction_id].push_back(event_info);
				}
			}

			// commitId=>events
			{
				const auto& commit_events_key = make_commit_events_key(commitId);
				const auto& events_json = ContractChanges::events_to_json(changes->events);
				if (!_db->Put(write_options, commit_events_key, jsondiff::json_dumps(events_json)).ok()) {
					BOOST_THROW_EXCEPTION(ContractStorageException("commit events save error"));
				}
				changed_leveldb_keys.push_back(commit_events_key);
			}
			// transactionId=>events
			for (const auto& p : *transaction_events) {
				const auto& tx_events_key = make_transaction_events_key(p.first);
				const auto& tx_events_json = ContractChanges::events_to_json(p.second);
				if (!_db->Put(write_options, tx_events_key, jsondiff::json_dumps(tx_events_json)).ok()) {
					BOOST_THROW_EXCEPTION(ContractStorageException("commit events save error"));
				}
				changed_leveldb_keys.push_back(tx_events_key);
			}

			// upgrade infos
			for (const auto& upgrade_info : changes->upgrade_infos)
			{
				const auto& contract_id = upgrade_info.contract_id;
				std::string value;
				auto contract_info_key = make_contract_info_key(contract_id);
				auto status = _db->Get(read_options, contract_info_key, &value);
				if (!status.ok()) {
					BOOST_THROW_EXCEPTION(ContractStorageException("contract info not found to upgrade"));
				}
				auto json_value = jsondiff::json_loads(value);
				if (!json_value.is_object())
					BOOST_THROW_EXCEPTION(ContractStorageException("contract info db data error"));
				auto contract_info = ContractInfo::from_json(json_value);
				auto old_contract_name(contract_info->name);
				if(!old_contract_name.empty())
					BOOST_THROW_EXCEPTION(ContractStorageException(std::string("contract ") + contract_id + " with name can't upgrade again"));
				if(upgrade_info.name_diff)
					contract_info->name = differ.patch(contract_info->name, upgrade_info.name_diff).as_string();
				if(upgrade_info.description_diff)
					contract_info->description = differ.patch(contract_info->description, upgrade_info.description_diff).as_string();
				const auto& new_contract_info_value = jsondiff::json_dumps(contract_info->to_json());
				auto write_status = _db->Put(write_options, contract_info_key, new_contract_info_value);
				if (!write_status.ok())
					BOOST_THROW_EXCEPTION(ContractStorageException("contract info write to db error"));
				changed_leveldb_keys.push_back(contract_info_key);

				if (!old_contract_name.empty()) {
					const auto& contract_name_id_mapping_key = make_contract_name_id_mapping_key(old_contract_name);
					auto delete_status = _db->Delete(write_options, contract_name_id_mapping_key);
					if (!delete_status.ok() && !delete_status.IsNotFound())
						BOOST_THROW_EXCEPTION(ContractStorageException("contract info write to db error"));
					changed_leveldb_keys.push_back(contract_name_id_mapping_key);
				}
				if (!contract_info->name.empty()) {
					const auto& contract_name_id_mapping_key = make_contract_name_id_mapping_key(contract_info->name);
					if (!_db->Put(write_options, contract_name_id_mapping_key, contract_info->id).ok())
						BOOST_THROW_EXCEPTION(ContractStorageException("contract info write to db error"));
					changed_leveldb_keys.push_back(contract_name_id_mapping_key);
				}
			}

			// save commit info
			const auto& diff_json = changes->to_json();
			const auto& diff_str = jsondiff::json_dumps(diff_json);
			add_commit_info(commitId, CONTRACT_STORAGE_CHANGE_TYPE, diff_str, "");
			if (!_db->Put(write_options, root_state_hash_key, root_state_hash).ok())
				BOOST_THROW_EXCEPTION(ContractStorageException("update root state hash error"));
			changed_leveldb_keys.push_back(root_state_hash_key);
			if (!_db->Put(write_options, top_root_state_hash_key, root_state_hash).ok())
				BOOST_THROW_EXCEPTION(ContractStorageException("update top root state hash error"));
			changed_leveldb_keys.push_back(top_root_state_hash_key);
			success = true;
			return commitId;
		}

		ContractCommitId ContractStorageService::top_commit_id() const
		{
			check_db();
			char *errMsg;
			jsondiff::JsonArray records;
			auto query_sql = "select id, commit_id, change_type, contract_id from commit_info order by id desc limit 1";
			auto status = sqlite3_exec(_sql_db, query_sql,
				&query_records_sql_callback, &records, &errMsg);
			if (status != SQLITE_OK)
			{
				std::string err_msg_str(errMsg);
				sqlite3_free(errMsg);
				BOOST_THROW_EXCEPTION(ContractStorageException(err_msg_str));
			}
			if (records.size() > 0)
				return records[0]["commit_id"].as_string();
			return EMPTY_COMMIT_ID;
		}

		ContractCommitId ContractStorageService::generate_next_root_hash(const std::string& old_root_state_hash, const fc::sha256& diff_hash) const
		{
			return fc::sha256::hash(old_root_state_hash + diff_hash.str() + std::to_string(_current_block_height)).str();
		}

		void ContractStorageService::reset_root_state_hash(const ContractCommitId& dest_commit_id)
		{
			check_db();
			auto commit_info = get_commit_info(dest_commit_id);
			if (!commit_info && dest_commit_id != EMPTY_COMMIT_ID)
				BOOST_THROW_EXCEPTION(ContractStorageException(std::string("Can't find commit ") + dest_commit_id));
			leveldb::WriteOptions write_options;
			if (!_db->Put(write_options, root_state_hash_key, dest_commit_id).ok())
				BOOST_THROW_EXCEPTION(ContractStorageException("update root state hash error"));
		}

		void ContractStorageService::rollback_to_root_state_hash_without_transactional(const ContractCommitId& dest_commit_id, std::vector<std::string>& changed_leveldb_keys)
		{
			check_db();
			leveldb::WriteOptions write_options;
			leveldb::ReadOptions read_options;
			// find all commits after this commit
			auto commit_info = get_commit_info(dest_commit_id);
			if (!commit_info && dest_commit_id != EMPTY_COMMIT_ID)
				BOOST_THROW_EXCEPTION(ContractStorageException(std::string("Can't find commit ") + dest_commit_id));
			char *errMsg;
			jsondiff::JsonArray records;
			std::string query_sql;
			if (dest_commit_id == EMPTY_COMMIT_ID)
				query_sql = "select id, commit_id, change_type, contract_id from commit_info order by id desc";
			else
				query_sql = std::string("select id, commit_id, change_type, contract_id from commit_info where id>") + std::to_string(commit_info->id) + " order by id desc";

			auto status = sqlite3_exec(_sql_db, query_sql.c_str(),
				&query_records_sql_callback, &records, &errMsg);
			if (status != SQLITE_OK)
			{
				std::string err_msg_str(errMsg);
				sqlite3_free(errMsg);
				BOOST_THROW_EXCEPTION(ContractStorageException(err_msg_str));
			}
			std::vector<ContractCommitInfo> newerCommitInfos;
			for (const auto &item_json : records)
			{
				jsondiff::JsonObject found_record = item_json.as<jsondiff::JsonObject>();
				ContractCommitInfo commit_info;
				commit_info.id = found_record["id"].as_uint64();
				commit_info.commit_id = found_record["commit_id"].as_string();
				commit_info.change_type = found_record["change_type"].as_string();
				commit_info.contract_id = found_record["contract_id"].as_string();
				newerCommitInfos.push_back(commit_info);
			}

			jsondiff::JsonDiff differ;

			// rollback contracts info, contract balances, contract storages, upgrade infos and events
			for (auto i = newerCommitInfos.begin(); i != newerCommitInfos.end(); i++)
			{
				if (i->change_type == CONTRACT_INFO_CHANGE_TYPE)
				{
					// contract info change rollback
					auto diff_json = get_json_value_by_key_or_null(i->commit_id);
					auto contract_info_diff = std::make_shared<jsondiff::DiffResult>(diff_json);
					auto contract_info = get_contract_info(i->contract_id);
					auto rollbakced_contract_info_json = differ.rollback(contract_info->to_json(), contract_info_diff);
					auto rollbakced_contract_info = ContractInfo::from_json(rollbakced_contract_info_json);
					if (!rollbakced_contract_info)
					{
						// delete this contract in db
						const auto& delete_key = make_contract_info_key(i->contract_id);
						auto delete_contract_status = _db->Delete(write_options, delete_key);
						if (!delete_contract_status.ok())
							BOOST_THROW_EXCEPTION(ContractStorageException("delete contract info from db error"));
						changed_leveldb_keys.push_back(delete_key);
					}
					else
					{
						// set older data
						const auto& set_key = make_contract_info_key(i->contract_id);
						auto update_status = _db->Put(write_options, set_key, jsondiff::json_dumps(rollbakced_contract_info->to_json()));
						if (!update_status.ok())
							BOOST_THROW_EXCEPTION(ContractStorageException("rollback contract info to db error"));
						changed_leveldb_keys.push_back(set_key);
					}
					if (contract_info && contract_info->name.size() > 0)
					{
						// when contract have name
						if (!rollbakced_contract_info || rollbakced_contract_info->name.empty())
						{
							// when not have name before, delete name => id mapping
							const auto& contract_name_id_mapping_key = make_contract_name_id_mapping_key(contract_info->name);
							if (!_db->Delete(write_options, contract_name_id_mapping_key).ok())
								BOOST_THROW_EXCEPTION(ContractStorageException("rollback contract info(delete contract name=>id mapping) to db error"));
							changed_leveldb_keys.push_back(contract_name_id_mapping_key);
						}
					}
				}
				else if (i->change_type == CONTRACT_STORAGE_CHANGE_TYPE)
				{
					// contract balance and storage chagne rollback
					auto diff_json = get_json_value_by_key_or_null(i->commit_id);
					auto changes = ContractChanges::from_json(diff_json.as<jsondiff::JsonObject>());
					for (const auto &balance_change : changes.balance_changes)
					{
						// balance change rollback
						if (!balance_change.is_contract)
							continue;
						std::string value;
						auto contract_info_key = make_contract_info_key(balance_change.address);
						auto status = _db->Get(read_options, contract_info_key, &value);
						if (!status.ok()) {
							BOOST_THROW_EXCEPTION(ContractStorageException("contract info not found to transfer balance"));
						}
						auto json_value = jsondiff::json_loads(value);
						if (!json_value.is_object())
							BOOST_THROW_EXCEPTION(ContractStorageException("contract info db data error"));
						auto contract_info = ContractInfo::from_json(json_value);
						auto balances = contract_info->balances;
						auto found_balance = false;
						for (auto &balance : balances)
						{
							if (balance.asset_id == balance_change.asset_id)
							{
								found_balance = true;
								balance.amount = balance_change.add ? (balance.amount - balance_change.amount) : (balance.amount + balance_change.amount);
								break;
							}
						}
						if (!found_balance)
						{
							ContractBalance balance;
							balance.amount = balance_change.add ? 0 : balance_change.amount;
							balance.asset_id = balance_change.asset_id;
							balances.push_back(balance);
						}
						contract_info->balances = balances;
						auto new_contract_info_value = jsondiff::json_dumps(contract_info->to_json());
						auto write_status = _db->Put(write_options, contract_info_key, new_contract_info_value);
						if (!write_status.ok())
							BOOST_THROW_EXCEPTION(ContractStorageException("contract info write to db error"));
						changed_leveldb_keys.push_back(contract_info_key);
					}
					for (const auto &storage_change : changes.storage_changes)
					{
						// storage change rollback
						const auto &contract_id = storage_change.contract_id;
						for (const auto &storage_change_item : storage_change.items)
						{
							auto storage_new_value = get_contract_storage(contract_id, storage_change_item.name);
							auto storage_value = differ.rollback(storage_new_value, storage_change_item.diff);
							auto key = make_contract_storage_key(contract_id, storage_change_item.name);
							auto status = _db->Put(write_options, key, jsondiff::json_dumps(storage_value));
							if (!status.ok())
								BOOST_THROW_EXCEPTION(ContractStorageException("contract storage write to db error"));
							changed_leveldb_keys.push_back(key);
						}
					}
					for (const auto& upgrade_info : changes.upgrade_infos)
					{
						const auto& contract_id = upgrade_info.contract_id;
						std::string value;
						auto contract_info_key = make_contract_info_key(contract_id);
						auto status = _db->Get(read_options, contract_info_key, &value);
						if (!status.ok()) {
							BOOST_THROW_EXCEPTION(ContractStorageException("contract info not found to rollback upgrade"));
						}
						auto json_value = jsondiff::json_loads(value);
						if (!json_value.is_object())
							BOOST_THROW_EXCEPTION(ContractStorageException("contract info db data error"));
						auto contract_info = ContractInfo::from_json(json_value);
						auto now_contract_name(contract_info->name);
						jsondiff::JsonValue old_contract_name;
						if (upgrade_info.name_diff)
							old_contract_name = differ.rollback(contract_info->name, upgrade_info.name_diff);
						else
							old_contract_name = contract_info->name;
						contract_info->name = old_contract_name.is_string() ? old_contract_name.as_string() : "";
						jsondiff::JsonValue old_contract_desc;
						if (upgrade_info.description_diff)
							old_contract_desc = differ.rollback(contract_info->description, upgrade_info.description_diff);
						else
							old_contract_desc = contract_info->description;
						contract_info->description = old_contract_desc.is_string() ? old_contract_desc.as_string() : "";
						status = _db->Put(write_options, contract_info_key, jsondiff::json_dumps(contract_info->to_json()));
						if (!status.ok())
							BOOST_THROW_EXCEPTION(ContractStorageException("contract upgrade info rollback failed"));
						changed_leveldb_keys.push_back(contract_info_key);
						// mapping name=>id
						if (!now_contract_name.empty()) {
							const auto& contract_name_id_mapping_key = make_contract_name_id_mapping_key(now_contract_name);
							auto delete_status = _db->Delete(write_options, contract_name_id_mapping_key);
							if (!delete_status.ok() && !delete_status.IsNotFound())
								BOOST_THROW_EXCEPTION(ContractStorageException("contract upgrade info rollback failed"));
							changed_leveldb_keys.push_back(contract_name_id_mapping_key);
						}
						if (!contract_info->name.empty()) {
							const auto& contract_name_id_mapping_key = make_contract_name_id_mapping_key(contract_info->name);
							if (!_db->Put(write_options, contract_name_id_mapping_key, contract_info->id).ok())
								BOOST_THROW_EXCEPTION(ContractStorageException("contract upgrade info rollback failed"));
							changed_leveldb_keys.push_back(contract_name_id_mapping_key);
						}
					}
					std::set<std::string> transaction_ids;
					for (size_t j = 0; j < changes.events.size(); j++) {
						const auto& event_info = changes.events[j];
						const auto& event_id = make_event_id(i->commit_id, j);
						const auto& commit_event_key = make_commit_event_key(i->commit_id, event_id);
						if (!_db->Delete(write_options, commit_event_key).ok()) {
							BOOST_THROW_EXCEPTION(ContractStorageException("rollback event info failed"));
						}
						changed_leveldb_keys.push_back(commit_event_key);
						if (!event_info.transaction_id.empty()) {
							transaction_ids.insert(event_info.transaction_id);
							const auto& transaction_event_key = make_transaction_event_key(event_info.transaction_id, event_id);
							if (!_db->Delete(write_options, transaction_event_key).ok()) {
								BOOST_THROW_EXCEPTION(ContractStorageException("rollback event info failed"));
							}
							changed_leveldb_keys.push_back(transaction_event_key);
						}
					}
					{
						// transactionId=>events delete
						for (const auto& txid : transaction_ids) {
							const auto& tx_events_key = make_transaction_events_key(txid);
							auto status = _db->Delete(write_options, tx_events_key);
							if (!status.ok() && !status.IsNotFound()) {
								BOOST_THROW_EXCEPTION(ContractStorageException("rollback commit events failed"));
							}
							if (status.ok())
								changed_leveldb_keys.push_back(tx_events_key);
						}
					}
					{
						// events key delete
						const auto& commit_events_key = make_commit_events_key(i->commit_id);
						auto delete_commit_events_key_status = _db->Delete(write_options, commit_events_key);
						if (!delete_commit_events_key_status.ok() && !delete_commit_events_key_status.IsNotFound()) {
							BOOST_THROW_EXCEPTION(ContractStorageException("rollback commit events failed"));
						}
						if (delete_commit_events_key_status.ok())
							changed_leveldb_keys.push_back(commit_events_key);
					}
				}
				else
				{
					BOOST_THROW_EXCEPTION(ContractStorageException(std::string("not supported change type ") + i->change_type));
				}

				// delete the rollbacked commit_info
				char *delete_commit_info_err_msg;
				std::string delete_commit_info_sql = std::string("delete from commit_info where commit_id='") + i->commit_id + "'";
				auto delete_commit_info_status = sqlite3_exec(_sql_db, delete_commit_info_sql.c_str(), &empty_sql_callback, nullptr, &delete_commit_info_err_msg);
				if (delete_commit_info_status != SQLITE_OK)
				{
					std::string err_msg_str(delete_commit_info_err_msg);
					sqlite3_free(delete_commit_info_err_msg);
					BOOST_THROW_EXCEPTION(ContractStorageException(err_msg_str));
				}

				// delete the rollbackedCommitId => value in db
				auto deleteCommitIdValueStatus = _db->Delete(write_options, i->commit_id);
				if (!deleteCommitIdValueStatus.ok())
					BOOST_THROW_EXCEPTION(ContractStorageException(std::string("delete commit ") + i->commit_id + " error"));
			}

			const auto& root_state_hash = dest_commit_id;
			if (!_db->Put(write_options, root_state_hash_key, root_state_hash).ok())
				BOOST_THROW_EXCEPTION(ContractStorageException("update root state hash error"));
			changed_leveldb_keys.push_back(root_state_hash_key);
			if (!_db->Put(write_options, top_root_state_hash_key, root_state_hash).ok())
				BOOST_THROW_EXCEPTION(ContractStorageException("update top root state hash error"));
			changed_leveldb_keys.push_back(top_root_state_hash_key);
		}

		void ContractStorageService::rollback_contract_state(const ContractCommitId& dest_commit_id)
		{
			check_db();
			
			bool success = false;
			leveldb::WriteOptions write_options;
			leveldb::ReadOptions read_options;
			auto snapshot = _db->GetSnapshot();
			std::vector<std::string> changed_leveldb_keys;
			begin_sql_transaction();
			BOOST_SCOPE_EXIT_ALL(&) {
				if (success)
				{
					commit_sql_transaction();
				}
				else
				{
					rollback_sql_transaction();
					rollback_leveldb_transaction(snapshot, changed_leveldb_keys);
				}
			};
			auto commit_info = get_commit_info(dest_commit_id);
			if (!commit_info && dest_commit_id != EMPTY_COMMIT_ID)
				BOOST_THROW_EXCEPTION(ContractStorageException(std::string("Can't find commit ") + dest_commit_id));
			rollback_to_root_state_hash_without_transactional(dest_commit_id, changed_leveldb_keys);
			success = true;
		}

	}
}
