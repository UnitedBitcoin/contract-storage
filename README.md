contract_storage
======
storage component for blockchain contract to manage contract bytecode, contract balances, contract storages, and contract change history

# Dependencies

* boost 1.55
* leveldb
* sqlite3
* https://github.com/cryptonomex/fc
* https://github.com/BlockLink/jsondiff-cpp
* OpenSSL

# Features

* manage contracts info and balances
* manage contract commits(balances and storages changes)
* manage contract operation rollback
* root state hash as commit-id