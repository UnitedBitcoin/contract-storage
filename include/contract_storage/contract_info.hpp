#pragma once
#include <string>
#include <vector>
#include <memory>
#include <cinttypes>
#include <jsondiff/jsondiff.h>

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
			static std::shared_ptr<ContractBalance> from_json(jsondiff::JsonValue json_value);
		};

		struct ContractInfo
		{
			std::vector<unsigned char> bytecode;
			AddressType id;
			std::string name;
			std::vector<std::string> apis;
			std::vector<std::string> offline_apis;
			std::vector<ContractBalance> balances;

			jsondiff::JsonObject to_json() const;
			static std::shared_ptr<ContractInfo> from_json(jsondiff::JsonValue json_value);
		};
		typedef std::shared_ptr<ContractInfo> ContractInfoP;

		
	}
}
