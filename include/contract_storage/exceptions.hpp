#pragma once
#include <string>
#include <exception>
#include <cstdint>
#include <memory>

namespace contract {
	namespace storage {
		class ContractStorageException : public std::exception
		{
		protected:
			int64_t _code;
			std::string _name_value;
			std::string _error_msg;
		public:
			inline ContractStorageException() : _code(0) { }
			inline ContractStorageException(int64_t code, const std::string &name_value, const std::string &error_msg)
				: _code(code), _name_value(name_value), _error_msg(error_msg) {}
			inline ContractStorageException(const ContractStorageException& other) {
				_code = other._code;
				_name_value = other._name_value;
				_error_msg = other._error_msg;
			}
			inline ContractStorageException(const char *msg)
			{
				_code = 1;
				_error_msg = msg;
			}
			inline ContractStorageException(const std::string &msg)
			{
				_code = 1;
				_error_msg = msg;
			}
			inline ContractStorageException& operator=(const ContractStorageException& other) {
				if (this != &other)
				{
					_error_msg = other._error_msg;
				}
				return *this;
			}
			inline virtual ~ContractStorageException() {}

#ifdef WIN32
			inline virtual const char* what() const
#else
			inline virtual const char* what() const noexcept
#endif 
			{
				return _error_msg.c_str();
			}
			inline virtual int64_t code() const {
				return _code;
			}
			inline std::string name() const
			{
				return _name_value;
			}
			inline virtual std::shared_ptr<ContractStorageException> dynamic_copy_exception()const
			{
				return std::make_shared<ContractStorageException>(*this);
			}
			inline virtual void dynamic_rethrow_exception()const
			{
				if (code() == 0)
					throw *this;
				else
					ContractStorageException::dynamic_rethrow_exception();
			}
		};
	}
}