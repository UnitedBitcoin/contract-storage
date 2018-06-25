#include <contract_storage/contract_storage.hpp>
#include <thread>
#include <chrono>
#include <iostream>
#include <fcrypto/base58.hpp>

using namespace contract::storage;
using namespace jsondiff;

static DiffResultP make_json_diff_of_string(JsonDiff &differ, const std::string &old_value, const std::string &new_value)
{
	auto value1 = JsonValue(old_value);
	auto value2 = JsonValue(new_value);
	return differ.diff(value1, value2);
}

int main(int argc, char **argv)
{
	// you need delete old test data to run this testcase
	JsonDiff differ;
	uint32_t magic_num = 123;
	std::string db_path("test_leveldb.db");
	std::string sqldb_path("test_sql_db.db");

	auto service = ContractStorageService::get_instance(magic_num, db_path, sqldb_path);
	service->open();
	service->clear_sql_db(); // for test usage
	auto contract_info = std::make_shared<ContractInfo>();
	contract_info->id = "c1";
	contract_info->name = "";
	contract_info->version = 1;
	contract_info->creator_address = "addr1";
	contract_info->txid = "txid-123";
	contract_info->is_native = false;
	contract_info->bytecode.resize(1);
	contract_info->bytecode[0] = 123;
	contract_info->apis.push_back("init");
	contract_info->apis.push_back("say");
	contract_info->offline_apis.push_back("query1");
	contract_info->offline_apis.push_back("name");
	auto commit1 = service->save_contract_info(contract_info);
	auto contract_info_found = service->get_contract_info(contract_info->id);

	contract_info->name = "hello1";
	auto commit1_after_change_name = service->save_contract_info(contract_info);
	auto contract_info_found_after_change_name = service->get_contract_info(contract_info->id);
	assert(contract_info_found_after_change_name->name == contract_info->name);

	service->rollback_contract_state(commit1);
	auto changes_of_change_contract_desc = std::make_shared<ContractChanges>();
	changes_of_change_contract_desc->upgrade_infos.resize(1);
	changes_of_change_contract_desc->upgrade_infos[0].contract_id = contract_info->id;
	std::string contract_desc("demo description 123");
	changes_of_change_contract_desc->upgrade_infos[0].description_diff = make_json_diff_of_string(differ, contract_info->description, contract_desc);
	auto commit1_after_change_contract_desc = service->commit_contract_changes(changes_of_change_contract_desc);
	auto contract_info_found_after_change_desc = service->get_contract_info(contract_info->id);
	assert(contract_desc == contract_info_found_after_change_desc->description);
	
	// commit changes
	auto changes1 = std::make_shared<ContractChanges>();
	ContractBalanceChange balance_change1;
	balance_change1.add = true;
	balance_change1.is_contract = true;
	balance_change1.address = contract_info->id;
	balance_change1.amount = 100;
	balance_change1.asset_id = 0;
	balance_change1.memo = "test memo";
	changes1->balance_changes.push_back(balance_change1);
	ContractStorageChange storage_change1;
	storage_change1.contract_id = contract_info->id;
	ContractStorageItemChange item_change1;
	item_change1.name = "name";
	item_change1.diff = make_json_diff_of_string(differ, "", "China");
	storage_change1.items.push_back(item_change1);
	changes1->storage_changes.push_back(storage_change1);
	changes1->events.push_back(ContractEventInfo{"tx1", "contract1", "hello", "world123"});

	auto commit_id_before_commit2 = commit1_after_change_contract_desc;
	auto commit2 = service->commit_contract_changes(changes1);

	// get balance and storage after commit
	auto balances_after_commit_changes1 = service->get_contract_balances(contract_info->id);
	assert(balances_after_commit_changes1.size() == 1 && balances_after_commit_changes1[0].amount == 100 && balances_after_commit_changes1[0].asset_id == 0);
	auto name_storage_after_commit_changes1 = service->get_contract_storage(contract_info->id, "name").as_string();
	assert(name_storage_after_commit_changes1 == "China");
	auto commit_events_after_commit_changes1 = service->get_commit_events(service->current_root_state_hash());
	auto transaction_events_after_commit_changes1 = service->get_transaction_events(changes1->events[0].transaction_id);
	assert(commit_events_after_commit_changes1->size() == 1);
	assert(transaction_events_after_commit_changes1->size() == 1);

	// rollback
	service->rollback_contract_state(commit_id_before_commit2);
	const auto& contract_info_after_rollbacked_to_commit_id_before_commit2 = service->get_contract_info(contract_info->id);

	auto commit2_again = service->commit_contract_changes(changes1);
	assert(commit2_again == commit2);
	service->reset_root_state_hash(commit_id_before_commit2);
	assert(commit_id_before_commit2 == service->current_root_state_hash());
	assert(commit2 == service->top_root_state_hash());
	// TODO: test get snapshot after current commit id
	auto commit2_again_again = service->commit_contract_changes(changes1);
	assert(commit2_again_again == commit2);

	service->rollback_contract_state(commit1);
	auto cur_root_hash = service->current_root_state_hash();
	assert(cur_root_hash == commit1);
	assert(service->get_contract_info(contract_info->id)->name == "");

	auto current_commit_id_after_rollback1 = service->current_root_state_hash();
	assert(current_commit_id_after_rollback1 == "0055314d90bd9aaa6b415106283928f9c06fb0d6ca5de5ce642a6dd520ff3b75");

	// get balance and storage after rollback
	auto balances_after_rollback1 = service->get_contract_balances(contract_info->id);
	auto name_storage_after_rollback1 = service->get_contract_storage(contract_info->id, "name").as_string();

	auto commit_events_after_rollback = service->get_commit_events(service->current_root_state_hash());
	auto transaction_events_after_rollback = service->get_transaction_events(changes1->events[0].transaction_id);
	assert(commit_events_after_rollback->size() == 0);
	assert(transaction_events_after_rollback->size() == 0);

	// rollback to contract not created
	service->rollback_contract_state(EMPTY_COMMIT_ID);

	// get contract info after rollback
	auto contract_info_after_rollback_all = service->get_contract_info(contract_info->id);
	assert(!contract_info_after_rollback_all);
	auto balances_after_rollback2 = service->get_contract_balances(contract_info->id);
	assert(balances_after_rollback2.empty());
	auto name_storage_after_rollback2 = service->get_contract_storage(contract_info->id, "name").as_string();
	assert(name_storage_after_rollback2 == "");

	{
		std::string hello("hello world");
		auto hello_base58 = fcrypto::to_base58(hello.c_str(), hello.size());
		auto hello_decodefrom58 = fcrypto::from_base58(hello_base58);
		auto hello_str_decoded = std::string(hello_decodefrom58.begin(), hello_decodefrom58.end());
		assert(hello_str_decoded == hello);
	}

	return 0;
}