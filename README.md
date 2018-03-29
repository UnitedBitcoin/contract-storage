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

* manage contracts info, balances, storages, events, etc.
* manage contract commits(changes of base info, balances, storages, events, etc.)
* manage contract operation rollback
* root state hash as commit-id
* reset current root state hash(looks like git's reset HEAD commit feature)