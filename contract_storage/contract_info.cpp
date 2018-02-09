#include <contract_storage/contract_info.hpp>
#include <fc/crypto/base64.hpp>
#include <memory>

namespace contract
{
	namespace storage
	{
		using namespace jsondiff;

		jsondiff::JsonObject ContractBalance::to_json() const
		{
			JsonObject balance_json;
			balance_json["asset_id"] = asset_id;
			balance_json["amount"] = amount;
			return balance_json;
		}
		std::shared_ptr<ContractBalance> ContractBalance::from_json(jsondiff::JsonValue json_value)
		{
			if (!json_value.is_object())
				return nullptr;
			auto json_obj = json_value.as<JsonObject>();
			auto balance = std::make_shared<ContractBalance>();
			balance->asset_id = (uint32_t)json_obj["asset_id"].as_uint64();
			balance->amount = json_obj["amount"].as_uint64();
			return balance;
		}

		jsondiff::JsonObject ContractInfo::to_json() const
		{
			JsonObject json_obj;
			json_obj["id"] = id;
			json_obj["name"] = name;
			json_obj["apis"] = apis;
			json_obj["offline_apis"] = offline_apis;
			JsonArray balances_array;
			for (const auto &balance : balances)
			{
				balances_array.push_back(balance.to_json());
			}
			json_obj["balances"] = balances_array;
			auto bytecode_base64 = fc::base64_encode(bytecode.data(), bytecode.size());
			json_obj["bytecode"] = bytecode_base64;
			return json_obj;
		}
		std::shared_ptr<ContractInfo> ContractInfo::from_json(jsondiff::JsonValue json_value)
		{
			if (json_value.is_null())
				return nullptr;
			auto json_obj = json_value.as<JsonObject>();
			if (json_obj.size() < 1)
				return nullptr;
			auto contract_info = std::make_shared<ContractInfo>();
			contract_info->id = json_obj["id"].as_string();
			contract_info->name = json_obj["name"].as_string();
			auto bytecode_base64 = json_obj["bytecode"].as_string();
			auto bytecode_str = fc::base64_decode(bytecode_base64);
			contract_info->bytecode.resize(bytecode_str.size());
			memcpy(contract_info->bytecode.data(), bytecode_str.c_str(), bytecode_str.size());
			auto apis_json_array = json_obj["apis"].as<jsondiff::JsonArray>();
			auto offline_apis_json_array = json_obj["offline_apis"].as<jsondiff::JsonArray>();
			for (size_t i = 0; i < apis_json_array.size(); i++)
			{
				contract_info->apis.push_back(apis_json_array[i].as_string());
			}
			for (size_t i = 0; i < offline_apis_json_array.size(); i++)
			{
				contract_info->offline_apis.push_back(offline_apis_json_array[i].as_string());
			}
			auto balances_json_array = json_obj["balances"].as<JsonArray>();
			for (const auto &balance_json : balances_json_array)
			{
				contract_info->balances.push_back(*(ContractBalance::from_json(balance_json)));
			}
			return contract_info;
		}
	}
}