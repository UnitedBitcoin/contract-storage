#pragma once
#include <string>
#include <memory>

namespace contract
{
	namespace storage
	{
		typedef std::string ContractCommitId;

		struct ContractCommitInfo
		{
			uint64_t id;
			ContractCommitId commit_id;
			std::string contract_id; // when is contract info change
			std::string change_type;
		};

		typedef std::shared_ptr<ContractCommitInfo> ContractCommitInfoP;

#define EMPTY_COMMIT_ID ""

		// to ensure commitId unique and reproducible, use outside commitId. you can store commitId in blockchain
	}
}
