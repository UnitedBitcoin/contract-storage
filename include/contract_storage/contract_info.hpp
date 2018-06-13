#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <cinttypes>
#include <jsondiff/jsondiff.h>
#include <fcrypto/sha256.hpp>

namespace contract
{
	namespace storage
	{
		typedef std::string AddressType;

		typedef uint64_t AmountType;

		struct ContractBalance
		{
			uint32_t asset_id;
			AmountType amount;

			jsondiff::JsonObject to_json() const;
			static std::shared_ptr<ContractBalance> from_json(const jsondiff::JsonValue& json_value);
		};

		struct ContractInfo
		{
			std::vector<unsigned char> bytecode;
			AddressType id;
			AddressType creator_address;
			
			std::string txid; // tx id where contract created in
			bool is_native = false; // whether this is native contract
			std::string contract_template_key; // only need when this contract is created from template

			std::string name;
			std::string description;
			uint32_t version = 0;
			std::vector<std::string> apis;
			std::vector<std::string> offline_apis;
			std::unordered_map<std::string, uint32_t> storage_types; // contract storage's types
			std::vector<ContractBalance> balances;

			jsondiff::JsonObject to_json() const;
			static std::shared_ptr<ContractInfo> from_json(const jsondiff::JsonValue& json_value);
		};
		typedef std::shared_ptr<ContractInfo> ContractInfoP;

		fcrypto::sha256 ordered_json_digest(const jsondiff::JsonValue& json_value);
		
	}
}
