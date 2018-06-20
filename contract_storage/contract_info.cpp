#include <contract_storage/contract_info.hpp>
#include <fjson/array.hpp>
#include <fcrypto/ripemd160.hpp>
#include <fcrypto/elliptic.hpp>
#include <openssl/sha.h>
#include <fcrypto/base58.hpp>
#include <fjson/crypto/base64.hpp>
#include <fcrypto/sha256.hpp>
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
			json_obj["version"] = version;
			json_obj["id"] = id;
			json_obj["creator_address"] = creator_address;
			json_obj["name"] = name;
			json_obj["description"] = description;
			json_obj["txid"] = txid;
			json_obj["is_native"] = is_native;
			json_obj["contract_template_key"] = contract_template_key;

			std::vector<std::string> ordered_apis(apis.begin(), apis.end());
			std::sort(ordered_apis.begin(), ordered_apis.end(), std::less<std::string>());
			json_obj["apis"] = ordered_apis;

			std::vector<std::string> ordered_offline_apis(offline_apis.begin(), offline_apis.end());
			std::sort(ordered_offline_apis.begin(), ordered_offline_apis.end(), std::less<std::string>());
			json_obj["offline_apis"] = ordered_offline_apis;

			JsonArray storages_array;

			// sort storages_array by name
			std::map<std::string, uint32_t, std::less<std::string>> ordered_storage_types(storage_types.begin(), storage_types.end());

			for(const auto& p : ordered_storage_types)
			{
				JsonArray item_array;
				item_array.push_back(p.first);
				item_array.push_back(p.second);
				storages_array.push_back(item_array);
			}
			json_obj["storage_types"] = storages_array;

			std::vector<ContractBalance> ordered_balances(balances.begin(), balances.end());
			std::sort(ordered_balances.begin(), ordered_balances.end(), [](const ContractBalance& a, const ContractBalance& b) {
				return a.asset_id - b.asset_id;
			});
			JsonArray balances_array;
			for (const auto &balance : ordered_balances)
			{
				if (balance.amount == 0)
					continue;
				balances_array.push_back(balance.to_json());
			}
			json_obj["balances"] = balances_array;

			auto bytecode_base64 = fjson::base64_encode(bytecode.data(), bytecode.size());
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
				if (json_obj["version"].is_integer())
					contract_info->version = json_obj["version"].as_uint64();
				contract_info->id = json_obj["id"].as_string();
				contract_info->name = json_obj["name"].as_string();
				if (json_obj.find("description") != json_obj.end())
					contract_info->description = json_obj["description"].as_string();
				if (json_obj.find("txid") != json_obj.end())
					contract_info->txid = json_obj["txid"].as_string();
				if (json_obj.find("is_native") != json_obj.end())
					contract_info->is_native = json_obj["is_native"].as_bool();
				if (json_obj.find("contract_template_key") != json_obj.end())
					contract_info->contract_template_key = json_obj["contract_template_key"].as_string();
				if (json_obj.find("creator_address") != json_obj.end())
					contract_info->creator_address = json_obj["creator_address"].as_string();
				auto bytecode_base64 = json_obj["bytecode"].as_string();
				auto bytecode_str = fjson::base64_decode(bytecode_base64);
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
						FJSON_ASSERT(item_json.size() >= 2, "contract info format error");
						contract_info->storage_types[item_json[0].as_string()] = item_json[1].as_uint64();
					}
				}
				if (json_obj["balances"].is_array())
				{
					auto balances_json_array = json_obj["balances"].as<JsonArray>();
					for (const auto &balance_json : balances_json_array)
					{
						auto balance = ContractBalance::from_json(balance_json);
						if (balance->amount == 0)
							continue;
						contract_info->balances.push_back(*balance);
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


		// ���������json object��ת����json array������json object/json array��Ԫ����Ҳ�ݹ鴦��
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

		static void sha256(char *string, char outputBuffer[65])
		{
			unsigned char hash[SHA256_DIGEST_LENGTH];
			SHA256_CTX sha256;
			SHA256_Init(&sha256);
			SHA256_Update(&sha256, string, strlen(string));
			SHA256_Final(hash, &sha256);
			int i = 0;
			for (i = 0; i < SHA256_DIGEST_LENGTH; i++)
			{
				sprintf(outputBuffer + (i * 2), "%02x", hash[i]);
			}
			outputBuffer[64] = 0;
		}

		fcrypto::sha256 ordered_json_digest(const jsondiff::JsonValue& json_value)
		{
			const auto& parsed_json = nested_json_object_to_array(json_value);
			const auto& dumped = json_dumps(parsed_json);
			return fcrypto::sha256::hash(dumped);
		}
	}
}