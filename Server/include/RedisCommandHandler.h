#ifndef REDIS_COMMAND_HANDLER_H
#define REDIS_COMMAND_HANDLER_H

#include<string>
#include<iostream>
#include<vector>
#include<sstream>
#include<algorithm>


class RedisCommandHandler{
    public:
    RedisCommandHandler();
    // Process a command from a client and return RESP-formatted response.
    std::string processCommand(const std::string& commandLine);
    private:

};

#endif // Ends REDIS_COMMAND_HANDLER_H