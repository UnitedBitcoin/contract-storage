#pragma once
#include <vector>
#include <contract_storage/config.hpp>
#include <contract_storage/contract_info.hpp>
#include <contract_storage/commit.hpp>
#include <contract_storage/change.hpp>
#include <boost/exception/all.hpp>
#include <fc/array.hpp>
#include <fc/crypto/ripemd160.hpp>
#include <fc/crypto/elliptic.hpp>
#include <fc/crypto/base58.hpp>
#include <fc/crypto/sha256.hpp>
#include <boost/uuid/sha1.hpp>
#include <exception>
#include <leveldb/db.h>
#include <sqlite3.h>

namespace contract
{
	namespace storage
	{
		class ContractStorageService
		{
		private:
			leveldb::DB *_db;
			sqlite3 *_sql_db;
			uint32_t _current_block_height;
			uint32_t _magic_number;
			std::string _storage_db_path;
			std::string _storage_sql_db_path;
		public:
			ContractStorageService(uint32_t magic_number, const std::string& storage_db_path, const std::string& storage_sql_db_path);
			~ContractStorageService();
			
			// these apis may throws boost::exception
			void open();
			void close();

			ContractInfoP get_contract_info(const AddressType& contract_id) const;
			ContractCommitId save_contract_info(ContractInfoP contract_info);

			jsondiff::JsonValue get_contract_storage(AddressType contract_id, const std::string& storage_name) const;
			std::vector<ContractBalance> get_contract_balances(const AddressType& contract_id) const;
			// you must ensure changes is right before commit now
			ContractCommitId commit_contract_changes(ContractChangesP changes);
			void rollback_contract_state(ContractCommitId dest_commit_id);

			// don't call this in production usage
			void clear_sql_db();

			// hash the all contract-storage world
			// new-root-hash = hash(old-root-hash, commit-diff, block_height)
			std::string current_root_state_hash() const;

			ContractCommitId current_commit_id() const;
			uint32_t magic_number() const { return _magic_number; }
			uint32_t current_block_height() const { return _current_block_height; }
			void set_current_block_height(uint32_t block_height) { this->_current_block_height = block_height; }

		private:
			// check db opened? if not, throw boost::exception
			void check_db() const;
			void begin_sql_transaction();
			void commit_sql_transaction();
			void rollback_sql_transaction();
			// init commits sql table
			void init_commits_table();
			ContractCommitInfoP get_commit_info(const ContractCommitId& commit_id);
			// add commit info to sql db
			void add_commit_info(ContractCommitId commit_id, const std::string &change_type, const std::string &diff_str, const std::string &contract_id);
			// get value from key-value db by key
			std::string get_value_by_key_or_error(const std::string &key);
			jsondiff::JsonValue get_json_value_by_key_or_null(const std::string &key);

			ContractCommitId generate_next_root_hash(const std::string& old_root_state_hash, const fc::sha256& diff_hash) const;

			// calculate new-contract-info commit
			fc::sha256 hash_new_contract_info_commit(ContractInfoP contract_info) const;
			fc::sha256 hash_contract_changes(ContractChangesP changes) const;
		};
	}
}
