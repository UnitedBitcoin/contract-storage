#pragma once
#include <vector>
#include <contract_storage/config.hpp>
#include <contract_storage/contract_info.hpp>
#include <contract_storage/commit.hpp>
#include <contract_storage/change.hpp>
#include <boost/exception/all.hpp>
#include <fjson/array.hpp>
#include <fcrypto/ripemd160.hpp>
#include <fcrypto/elliptic.hpp>
#include <fcrypto/base58.hpp>
#include <fcrypto/sha256.hpp>
#include <boost/uuid/sha1.hpp>
#include <exception>
#include <memory>
#include <leveldb/db.h>
#include <sqlite3.h>

namespace contract
{
	namespace storage
	{
		class ContractStorageService final
		{
		private:
			leveldb::DB *_db;
			sqlite3 *_sql_db;
			uint32_t _current_block_height = 0;
			uint32_t _magic_number;
			std::string _storage_db_path;
			std::string _storage_sql_db_path;
		public:
			// suggest use get_instance
			ContractStorageService(uint32_t magic_number, const std::string& storage_db_path, const std::string& storage_sql_db_path, bool auto_open = true);
			~ContractStorageService();

			static std::shared_ptr<ContractStorageService> get_instance(uint32_t magic_number, const std::string& storage_db_path, const std::string& storage_sql_db_path);
			
			// these apis may throws boost::exception
			void open();
			void close();
			bool is_open() const;

			ContractInfoP get_contract_info(const AddressType& contract_id) const;
			ContractCommitId save_contract_info(ContractInfoP contract_info);
			AddressType find_contract_id_by_name(const std::string& name) const;

			jsondiff::JsonValue get_contract_storage(AddressType contract_id, const std::string& storage_name) const;
			std::vector<ContractBalance> get_contract_balances(const AddressType& contract_id) const;
			std::shared_ptr<std::vector<ContractEventInfo>> get_commit_events(const ContractCommitId& commit_id) const;
			std::shared_ptr<std::vector<ContractEventInfo>> get_transaction_events(const std::string& transaction_id) const;

			// you must ensure changes is right before commit now
			ContractCommitId commit_contract_changes(ContractChangesP changes);
			void rollback_contract_state(const ContractCommitId& dest_commit_id);

			// don't call this in production usage
			void clear_sql_db();

			// hash the all contract-storage world
			// new-root-hash = hash(old-root-hash, commit-diff, block_height)
			ContractCommitId current_root_state_hash() const;
			// whethe other_root_state_hash in history to current_root_state_hash
			bool is_current_root_state_hash_after(const ContractCommitId& other_root_state_hash) const;

			// check whether haven't pending reset root state hash
			bool is_latest() const;

			// TODO: get snapshot after commit id

			ContractCommitId top_root_state_hash() const;
			void reset_root_state_hash(const ContractCommitId& dest_commit_id);

			ContractCommitId top_commit_id() const;
			uint32_t magic_number() const { return _magic_number; }
			uint32_t current_block_height() const { return _current_block_height; }
			void set_current_block_height(uint32_t block_height) { this->_current_block_height = block_height; }

		private:
			// check db opened? if not, throw boost::exception
			void check_db() const;
			void begin_sql_transaction();
			void commit_sql_transaction();
			void rollback_sql_transaction();
			void rollback_leveldb_transaction(const leveldb::Snapshot* snapshot_to_rollback, const std::vector<std::string>& changed_keys);
			void rollback_to_root_state_hash_without_transactional(const ContractCommitId& dest_commit_id, std::vector<std::string>& changed_leveldb_keys);
			// init commits sql table
			void init_commits_table();
			ContractCommitInfoP get_commit_info(const ContractCommitId& commit_id) const;
			// add commit info to sql db
			void add_commit_info(ContractCommitId commit_id, const std::string &change_type, const std::string &diff_str, const std::string &contract_id);
			// get value from key-value db by key
			std::string get_value_by_key_or_error(const std::string &key);
			jsondiff::JsonValue get_json_value_by_key_or_null(const std::string &key);

			ContractCommitId generate_next_root_hash(const std::string& old_root_state_hash, const fcrypto::sha256& diff_hash) const;

			// calculate new-contract-info commit
			fcrypto::sha256 hash_new_contract_info_commit(ContractInfoP contract_info) const;
			fcrypto::sha256 hash_contract_changes(ContractChangesP changes) const;
		};
	}
}
