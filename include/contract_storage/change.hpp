#pragma once
#include <string>
#include <vector>
#include <contract_storage/contract_info.hpp>
#include <jsondiff/jsondiff.h>

namespace contract
{
	namespace storage
	{
		struct ContractBalanceChange
		{
			uint32_t asset_id;
			AddressType address;
			AmountType amount;
			bool add; // true: + balance, false, - balance
			bool is_contract;
			std::string memo;

			ContractBalanceChange();

			jsondiff::JsonObject to_json() const;
			static ContractBalanceChange from_json(const jsondiff::JsonObject& json_obj);
		};
		struct ContractStorageItemChange
		{
			std::string name;
			jsondiff::DiffResultP diff;
		};
		struct ContractStorageChange
		{
			AddressType contract_id;
			std::vector<ContractStorageItemChange> items;

			jsondiff::JsonObject to_json() const;
			static ContractStorageChange from_json(const jsondiff::JsonObject& json_obj);
		};
		struct ContractEventInfo
		{
			std::string transaction_id;
			AddressType contract_id;
			std::string event_name;
			std::string event_arg;

			jsondiff::JsonObject to_json() const;
			static ContractEventInfo from_json(const jsondiff::JsonObject& json_obj);
		};
		struct ContractUpgradeInfo
		{
			AddressType contract_id;
			jsondiff::DiffResultP name_diff;
			jsondiff::DiffResultP description_diff;

			jsondiff::JsonObject to_json() const;
			static ContractUpgradeInfo from_json(const jsondiff::JsonObject& json_obj);
		};
		struct ContractChanges
		{
			// balances changes and storage changes
			std::vector<ContractBalanceChange> balance_changes;
			std::vector<ContractStorageChange> storage_changes;
			std::vector<ContractEventInfo> events;
			std::vector<ContractUpgradeInfo> upgrade_infos;
			// TODO: chainsql changes

			jsondiff::JsonObject to_json() const;
			static ContractChanges from_json(const jsondiff::JsonObject& json_obj);

			static jsondiff::JsonArray events_to_json(const std::vector<ContractEventInfo>& events);
			static std::vector<ContractEventInfo> events_from_json(const jsondiff::JsonArray& events_json_array);

			bool empty() const;
		};
		typedef std::shared_ptr<ContractChanges> ContractChangesP;

#define CONTRACT_INFO_CHANGE_TYPE "contract_info"
#define CONTRACT_STORAGE_CHANGE_TYPE "storage_change"

	}
}
