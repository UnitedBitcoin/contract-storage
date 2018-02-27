#include <contract_storage/contract_info.hpp>
#include <fc/array.hpp>
#include <fc/crypto/ripemd160.hpp>
#include <fc/crypto/elliptic.hpp>
#include <fc/crypto/base58.hpp>
#include <fc/crypto/base64.hpp>
#include <fc/crypto/sha256.hpp>
#include <boost/uuid/sha1.hpp>
#include <memory>
#include <list>

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
		std::shared_ptr<ContractBalance> ContractBalance::from_json(const jsondiff::JsonValue& json_value)
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
			JsonArray storages_array;
			for(const auto& p : storage_types)
			{
				JsonArray item_array;
				item_array.push_back(p.first);
				item_array.push_back(p.second);
				storages_array.push_back(item_array);
			}
			json_obj["storage_types"] = storages_array;

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
		std::shared_ptr<ContractInfo> ContractInfo::from_json(const jsondiff::JsonValue& json_value)
		{
			if (json_value.is_null())
				return nullptr;
			auto json_obj = json_value.as<JsonObject>();
			if (json_obj.size() < 1)
				return nullptr;
			try
			{
				auto contract_info = std::make_shared<ContractInfo>();
				contract_info->id = json_obj["id"].as_string();
				contract_info->name = json_obj["name"].as_string();
				auto bytecode_base64 = json_obj["bytecode"].as_string();
				auto bytecode_str = fc::base64_decode(bytecode_base64);
				contract_info->bytecode.resize(bytecode_str.size());
				memcpy(contract_info->bytecode.data(), bytecode_str.c_str(), bytecode_str.size());
				auto apis_json_array = json_obj["apis"].as<JsonArray>();
				auto offline_apis_json_array = json_obj["offline_apis"].as<JsonArray>();
				for (size_t i = 0; i < apis_json_array.size(); i++)
				{
					contract_info->apis.push_back(apis_json_array[i].as_string());
				}
				for (size_t i = 0; i < offline_apis_json_array.size(); i++)
				{
					contract_info->offline_apis.push_back(offline_apis_json_array[i].as_string());
				}
				if (json_obj["storage_types"].is_array())
				{
					auto storage_types_json_array = json_obj["storage_types"].as<JsonArray>();
					for (size_t i = 0; i < storage_types_json_array.size(); i++)
					{
						auto item_json = storage_types_json_array[i].as<JsonArray>();
						FC_ASSERT(item_json.size() >= 2, "contract info format error");
						contract_info->storage_types[item_json[0].as_string()] = item_json[1].as_uint64();
					}
				}
				if (json_obj["balances"].is_array())
				{
					auto balances_json_array = json_obj["balances"].as<JsonArray>();
					for (const auto &balance_json : balances_json_array)
					{
						contract_info->balances.push_back(*(ContractBalance::from_json(balance_json)));
					}
				}
				return contract_info;
			}
			catch (const std::exception& e)
			{
				return nullptr;
			}
		}

		static bool compare_key(const std::string& first, const std::string& second)
		{
			unsigned int i = 0;
			while ((i<first.length()) && (i<second.length()))
			{
				if (first[i] < second[i]) 
					return true;
				else if (first[i] > second[i]) 
					return false;
				else 
					++i;
			}
			return (first.length() < second.length());
		}


		// 如果参数是json object，转换成json array，并且json object/json array的元素中也递归处理
		static jsondiff::JsonValue nested_json_object_to_array(const jsondiff::JsonValue& json_value)
		{
			if (json_value.is_object())
			{
				const auto& obj = json_value.as<jsondiff::JsonObject>();
				jsondiff::JsonArray json_array;
				std::list<std::string> keys;
				for (auto it = obj.begin(); it != obj.end(); it++)
				{
					keys.push_back(it->key());
				}
				keys.sort(&compare_key);
				for (const auto& key : keys)
				{
					jsondiff::JsonArray item_json;
					item_json.push_back(key);
					item_json.push_back(nested_json_object_to_array(obj[key]));
					json_array.push_back(item_json);
				}
				return json_array;
			}
			if (json_value.is_array())
			{
				const auto& arr = json_value.as<jsondiff::JsonArray>();
				jsondiff::JsonArray result;
				for (const auto& item : arr)
				{
					result.push_back(nested_json_object_to_array(item));
				}
				return result;
			}
			return json_value;
		}

		fc::sha256 ordered_json_digest(const jsondiff::JsonValue& json_value)
		{
			const auto& parsed_json = nested_json_object_to_array(json_value);
			return fc::sha256::hash(json_dumps(parsed_json));
		}
	}
}