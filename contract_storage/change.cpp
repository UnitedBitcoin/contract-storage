#include <contract_storage/change.hpp>

namespace contract
{
	namespace storage
	{
		ContractBalanceChange::ContractBalanceChange()
			: add(false), is_contract(false), asset_id(0)
		{
		}

		jsondiff::JsonObject ContractBalanceChange::to_json() const
		{
			jsondiff::JsonObject json_obj;
			json_obj["asset_id"] = asset_id;
			json_obj["address"] = address;
			json_obj["amount"] = amount;
			json_obj["add"] = add;
			json_obj["is_contract"] = is_contract;
			json_obj["memo"] = memo;
			return json_obj;
		}
		ContractBalanceChange ContractBalanceChange::from_json(const jsondiff::JsonObject& json_obj)
		{
			ContractBalanceChange change;
			change.asset_id = (uint32_t) json_obj["asset_id"].as_uint64();
			change.address = json_obj["address"].as_string();
			change.amount = json_obj["amount"].as_uint64();
			change.add = json_obj["add"].as_bool();
			change.is_contract = json_obj["is_contract"].as_bool();
			change.memo = json_obj["memo"].as_string();
			return change;
		}

		jsondiff::JsonObject ContractStorageChange::to_json() const
		{
			jsondiff::JsonObject json_obj;
			json_obj["contract_id"] = contract_id;
			jsondiff::JsonArray items_array;
			for (const auto &item : items)
			{
				jsondiff::JsonObject item_obj;
				item_obj["name"] = item.name;
				item_obj["diff"] = item.diff->value();
				items_array.push_back(item_obj);
			}
			json_obj["items"] = items_array;
			return json_obj;
		}
		ContractStorageChange ContractStorageChange::from_json(const jsondiff::JsonObject& json_obj)
		{
			ContractStorageChange change;
			change.contract_id = json_obj["contract_id"].as_string();
			auto items_array = json_obj["items"].as<jsondiff::JsonArray>();
			for (size_t i = 0; i < items_array.size(); i++)
			{
				auto item_obj = items_array[i].as<jsondiff::JsonObject>();
				ContractStorageItemChange item_change;
				item_change.name = item_obj["name"].as_string();
				if (item_obj.find("diff") != item_obj.end())
					item_change.diff = std::make_shared<jsondiff::DiffResult>(item_obj["diff"]);
				else
					item_change.diff = std::make_shared<jsondiff::DiffResult>(nullptr);
				change.items.push_back(item_change);
			}
			return change;
		}

		jsondiff::JsonObject ContractEventInfo::to_json() const
		{
			jsondiff::JsonObject json_obj;
			json_obj["tx_id"] = transaction_id;
			json_obj["contract_id"] = contract_id;
			json_obj["name"] = event_name;
			json_obj["arg"] = event_arg;
			return json_obj;
		}
		ContractEventInfo ContractEventInfo::from_json(const jsondiff::JsonObject& json_obj)
		{
			ContractEventInfo event_info;
			event_info.transaction_id = json_obj["tx_id"].as_string();
			event_info.contract_id = json_obj["contract_id"].as_string();
			event_info.event_name = json_obj["name"].as_string();
			event_info.event_arg = json_obj["arg"].as_string();
			return event_info;
		}

		jsondiff::JsonObject ContractUpgradeInfo::to_json() const
		{
			jsondiff::JsonObject json_obj;
			json_obj["contract_id"] = contract_id;
			if(name_diff)
				json_obj["name_diff"] = name_diff->value();
			if(description_diff)
				json_obj["description_diff"] = description_diff->value();
			return json_obj;
		}
		ContractUpgradeInfo ContractUpgradeInfo::from_json(const jsondiff::JsonObject& json_obj)
		{
			ContractUpgradeInfo info;
			info.contract_id = json_obj["contract_id"].as_string();
			if(json_obj.find("name_diff") != json_obj.end())
				info.name_diff = std::make_shared<jsondiff::DiffResult>(json_obj["name_diff"]);
			if(json_obj.find("description_diff") != json_obj.end())
				info.description_diff = std::make_shared<jsondiff::DiffResult>(json_obj["description_diff"]);
			return info;
		}

		jsondiff::JsonArray ContractChanges::events_to_json(const std::vector<ContractEventInfo>& events) {
			jsondiff::JsonArray events_array;
			for (const auto& event_info : events)
			{
				events_array.push_back(event_info.to_json());
			}
			return events_array;
		}
		std::vector<ContractEventInfo> ContractChanges::events_from_json(const jsondiff::JsonArray& events_json_array) {
			std::vector<ContractEventInfo> events;
			for (const auto& item : events_json_array)
			{
				events.push_back(ContractEventInfo::from_json(item.as<jsondiff::JsonObject>()));
			}
			return events;
		}

		jsondiff::JsonObject ContractChanges::to_json() const
		{
			jsondiff::JsonObject json_obj;
			jsondiff::JsonArray balance_changes_array;
			jsondiff::JsonArray storage_changes_array;
			jsondiff::JsonArray upgrade_infos_array;
			for (const auto &item : balance_changes)
			{
				balance_changes_array.push_back(item.to_json());
			}
			json_obj["balance_changes"] = balance_changes_array;
			for (const auto &item : storage_changes)
			{
				storage_changes_array.push_back(item.to_json());
			}
			json_obj["storage_changes"] = storage_changes_array;
			json_obj["events"] = events_to_json(events);
			for (const auto& info : upgrade_infos)
			{
				upgrade_infos_array.push_back(info.to_json());
			}
			json_obj["upgrade_infos"] = upgrade_infos_array;
			return json_obj;
		}

		bool ContractChanges::empty() const {
			return balance_changes.empty() && storage_changes.empty() && events.empty() && upgrade_infos.empty();
		}

		ContractChanges ContractChanges::from_json(const jsondiff::JsonObject& json_obj)
		{
			ContractChanges changes;
			const auto &balance_changes_array = json_obj["balance_changes"].as<jsondiff::JsonArray>();
			const auto &storage_changes_array = json_obj["storage_changes"].as<jsondiff::JsonArray>();
			for (const auto &item : balance_changes_array)
			{
				changes.balance_changes.push_back(ContractBalanceChange::from_json(item.as<jsondiff::JsonObject>()));
			}
			for (const auto &item : storage_changes_array)
			{
				changes.storage_changes.push_back(ContractStorageChange::from_json(item.as<jsondiff::JsonObject>()));
			}
			if (json_obj.find("events") != json_obj.end())
			{
				const auto& events_array = json_obj["events"].as<jsondiff::JsonArray>();
				changes.events = events_from_json(events_array);
			}
			if (json_obj.find("upgrade_infos") != json_obj.end())
			{
				const auto& upgrade_infos_array = json_obj["upgrade_infos"].as<jsondiff::JsonArray>();
				for (const auto& item : upgrade_infos_array) {
					changes.upgrade_infos.push_back(ContractUpgradeInfo::from_json(item.as<jsondiff::JsonObject>()));
				}
			}
			return changes;
		}

	}
}
