#pragma once

#include <fjson/fwd.hpp>
#include <fjson/io/raw_fwd.hpp>
#include <fjson/reflect/typename.hpp>

namespace fcrypto {
class sha512;
class sha256;
using fjson::string;

class ripemd160
{
  public:
    ripemd160();
    explicit ripemd160( const string& hex_str );

    string str()const;
    explicit operator string()const;

    char*    data()const;
    size_t data_size()const { return 160/8; }

    static ripemd160 hash( const fcrypto::sha512& h );
    static ripemd160 hash( const fcrypto::sha256& h );
    static ripemd160 hash( const char* d, uint32_t dlen );
    static ripemd160 hash( const string& );

    template<typename T>
    static ripemd160 hash( const T& t ) 
    { 
      ripemd160::encoder e; 
      fjson::raw::pack(e,t);
      return e.result(); 
    } 

    class encoder 
    {
      public:
        encoder();
        ~encoder();

        void write( const char* d, uint32_t dlen );
        void put( char c ) { write( &c, 1 ); }
        void reset();
        ripemd160 result();

      private:
        struct      impl;
        fjson::fwd<impl,96> my;
    };

    template<typename T>
    inline friend T& operator<<( T& ds, const ripemd160& ep ) {
      ds.write( ep.data(), sizeof(ep) );
      return ds;
    }

    template<typename T>
    inline friend T& operator>>( T& ds, ripemd160& ep ) {
      ds.read( ep.data(), sizeof(ep) );
      return ds;
    }
    friend ripemd160 operator << ( const ripemd160& h1, uint32_t i       );
    friend bool      operator == ( const ripemd160& h1, const ripemd160& h2 );
    friend bool      operator != ( const ripemd160& h1, const ripemd160& h2 );
    friend ripemd160 operator ^  ( const ripemd160& h1, const ripemd160& h2 );
    friend bool      operator >= ( const ripemd160& h1, const ripemd160& h2 );
    friend bool      operator >  ( const ripemd160& h1, const ripemd160& h2 );
    friend bool      operator <  ( const ripemd160& h1, const ripemd160& h2 );

    uint32_t _hash[5];
};

  class variant;
  void to_variant( const ripemd160& bi, variant& v );
  void from_variant( const variant& v, ripemd160& bi );

  typedef ripemd160 uint160_t;
  typedef ripemd160 uint160;

} // namespace fcrypto

namespace fjson {
	template<> struct get_typename<fcrypto::uint160_t> { static const char* name() { return "uint160_t"; } };
}

namespace std
{
    template<>
    struct hash<fcrypto::ripemd160>
    {
       size_t operator()( const fcrypto::ripemd160& s )const
       {
           return  *((size_t*)&s);
       }
    };
}
