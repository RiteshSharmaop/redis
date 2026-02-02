#ifndef RESPONSEPARSER_H
#define RESPONSEPARSER_H

#include<string>

class ResponseParser {
    public:
        // Read from given socket and return parsed response a string, return "" in failure.
        static std::string parseResponse(int sockfd);
    private:
        // Redis Serialization Protocol 2
        static std::string parseSimpleString(int sockfd);
        static std::string parseSimpleErrors(int sockfd);
        static std::string parseInteger(int sockfd);
        static std::string parseBulkString(int sockfd);
        static std::string parseArray(int sockfd);
};  

#endif // ends RESPONSEPARSER_H