 // #include <./include/RedisCommandHandler.h>
#include "../include/RedisCommandHandler.h"
#include "../include/RedisDatabase.h"
// RESP parser:
// *2\r\n$4\r\n\PING\r\n$4\r\nTEST\r\n
// *2 -> array has 2 elements
// $4 -> next string has 4 characters
// PING
// TEST


std::vector<std::string> parseRespCommand(const std::string &input) {
    std::vector<std::string> tokens;
    
    if(input.empty()) return tokens;
    
    // If it dose start with '*', fallback to splitting by whitespaces.
    if(input[0] != '*'){
        std::istringstream iss(input);
        std::string token;
        while(iss >> token){
            tokens.push_back(token);
        }
        return tokens;
    }  

    size_t pos = 0;
    // Expect '*' followed by number of elements
    if(input[pos] != '*') {
        return tokens;
    }
    pos++;  //skip '*'

    // crlf = Carriage Return (\r), Line Feed (\n)
    size_t crlf = input.find("\r\n" , pos);
    if(crlf == std::string::npos)  return tokens;

    int numElements = std::stoi(input.substr(pos , crlf - pos));
    pos = crlf + 2;
    
    for(int i = 0; i < numElements ; ++i){
        if(pos >= input.size() || input[pos] != '$') break;  //fomat error
        pos++; // skil '$'

        crlf = input.find("\r\n" , pos);
        if(crlf == std::string::npos) break;
        int len = std::stoi(input.substr(pos , crlf - pos));
        pos = crlf + 2;

        if(pos + len > input.size()) break;
        std::string token = input.substr(pos , len);
        tokens.push_back(token);
        pos += len + 2; // skip token and CRLF
    }
    return tokens;
}


// common commands: PING, ECHO, FLUSHALL
static std::string handlePing(const std::vector<std::string> &tokens){
    return "+PONG\r\n";
}

static std::string handleEcho(const std::vector<std::string> &tokens){
    if(tokens.size() < 2) return "-Error: ECHO requires a message\r\n";
    return "+" + tokens[1] + "\r\n";
}

static std::string handleFlushAll(RedisDatabase &db){
    db.flushAll();
    return "+OK\r\n";
}

static std::string handleDev(){
    return "+Ritesh Sharma -github: www.github.com/RiteshSharmaop\r\n";
}


// Key Value Operations: SET, GET, KEYS, TYPE, DEL, EXPIRE, RENAME

static std::string handleSet(RedisDatabase &db , const std::vector<std::string> &tokens){
    if(tokens.size() < 3) return "-Error: SET requires key and value\r\n";
    db.set(tokens[1] , tokens[2]);
    return "+OK\r\n";
}

static std::string handleGet(RedisDatabase &db , const std::vector<std::string> &tokens){
    if(tokens.size() < 2) return "-Error: GET requires key\r\n";
    std::string value;
    if(db.get(tokens[1] , value))
        return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
    else 
        return "$-1\r\n"; // key not found
}

static std::string handleKeys(RedisDatabase &db){
    std::vector<std::string> allKeys = db.keys();
    std::ostringstream response;
    response << "*" << allKeys.size() << "\r\n";
    for(const auto &key : allKeys)
        response << "$" << key.size() << "\r\n" << key << "\r\n";
    return response.str();
}

static std::string handleType(RedisDatabase &db , const std::vector<std::string> &tokens){
    if(tokens.size() < 2) return "-Error: TYPE requires key\r\n";
    return "+" + db.type(tokens[1]) + "\r\n";
}

static std::string handleDel(RedisDatabase &db , const std::vector<std::string> &tokens){
    if(tokens.size() < 2) return "-Error: DEL requires key\r\n";
    bool res = db.del(tokens[1]);
    return ":" + std::to_string(res ? 1 : 0) + "\r\n";
}

static std::string handleExpire(RedisDatabase &db , const std::vector<std::string> &args){
    if(args.size() < 3) return "-Error: EXPIRE requires key and time in seconds\r\n";
    if(db.expire(args[1] , args[2]))
        return "+OK\r\n";
    else 
        return "-Error: Failed to set expire\r\n";
}

static std::string handleRename(RedisDatabase &db , const std::vector<std::string> &tokens){
    if(tokens.size() < 3) return "-Error: RENAME requires old key name and new key name\r\n";
    if(db.rename(tokens[1] , tokens[2]))
        return "+OK\r\n";
    else 
        return "-Error: Failed to rename key\r\n";
}


// List Operations: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN

static std::string handleLPush(RedisDatabase &db , const std::vector<std::string> &tokens){  
    if(tokens.size() < 3) return "-Error: LPUSH requires key and value\r\n";
    db.lpush(tokens[1] , tokens[2]);
    ssize_t len = db.llen(tokens[1]);
    return ":" + std::to_string(len) + "\r\n";
}

static std::string handleRPush(RedisDatabase &db , const std::vector<std::string> &tokens){  
    if(tokens.size() < 3) 
        return "-Error: RPUSH requires key and value\r\n";
    db.rpush(tokens[1] , tokens[2]);
    ssize_t len = db.llen(tokens[1]);
    return ":" + std::to_string(len) + "\r\n";
}

static std::string handleLPop(RedisDatabase &db , const std::vector<std::string> &tokens){  
    if(tokens.size() < 2) 
        return "-Error: LPOP requires key\r\n";
    std::string value;
    if(db.lpop(tokens[1] , value))
        return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
    else 
        return "$-1\r\n"; // key not found or list empty
}

static std::string handleRPop(RedisDatabase &db , const std::vector<std::string> &tokens){  
    if(tokens.size() < 2) 
        return "-Error: RPOP requires key\r\n";
    std::string value;
    if(db.rpop(tokens[1] , value))
        return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
    else 
        return "$-1\r\n"; // key not found or list empty
}

static std::string handleLRange(RedisDatabase &db , const std::vector<std::string> &tokens){  
    if(tokens.size() < 4) return "-Error: LRANGE requires key, start and stop\r\n";
    std::vector<std::string> values;
    if(db.lrange(tokens[1] , tokens[2] , tokens[3] , values)){
        std::ostringstream response;
        response << "*" << values.size() << "\r\n";
        for(const auto &val : values)
            response << "$" << val.size() << "\r\n" << val << "\r\n";
        return response.str();
    }else {
        return "$-1\r\n"; // key not found or list empty
    }
}

static std::string handleLLen(RedisDatabase &db , const std::vector<std::string> &tokens){  
    if(tokens.size() < 2) 
        return "-Error: LLEN requires key\r\n";
    ssize_t len = db.llen(tokens[1]);
    if(len >= 0){
        return ":" + std::to_string(len) + "\r\n";
    }else {
        return "$-1\r\n"; // key not found or list empty
    }
}

static std::string handleLRem(RedisDatabase &db , const std::vector<std::string> &tokens){  
    if(tokens.size() < 4) 
        return "-Error: LREM requires key, count and value\r\n";
    
    try{
        int count = std::stoi(tokens[3]);
        int removed = db.lrem(tokens[1] , tokens[2] , count);
        return ":" + std::to_string(removed) + "\r\n";
    }catch(const std::invalid_argument& e){
        return "-Error: Invalid count value\r\n";
    }
}

static std::string handleLIndex(RedisDatabase &db , const std::vector<std::string> &tokens){  
    if(tokens.size() < 3) 
        return "-Error: LINDEX requires key and index\r\n";
    try{
        int index = std::stoi(tokens[2]);
        std::string value;
        if(db.lindex(tokens[1] , index , value))
            return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
        else 
            return "$-1\r\n"; // key not found or list empty
    }catch(const std::invalid_argument& e){
        return "-Error: Invalid index value\r\n";
    }
}

static std::string handleLSet(RedisDatabase &db , const std::vector<std::string> &tokens){  
    if(tokens.size() < 4) 
        return "-Error: LSET requires key, index and value\r\n";
    
    try{
        int index = std::stoi(tokens[2]);
        if(db.lset(tokens[1] , index , tokens[3]))
            return "+OK\r\n";
        else 
            return "-Error: Failed to set list element\r\n"; // key not found or index out of range
    }catch(const std::invalid_argument& e){
        return "-Error: Invalid index value\r\n";
    }
}



static std::string handleUnknownCommand(){
    return "-Error: Unknown command\r\n";
}




RedisCommandHandler::RedisCommandHandler(){

}

std::string RedisCommandHandler::processCommand(const std::string& commandLine){
    // Use  RESP parser
    std::vector<std::string> tokens = parseRespCommand(commandLine);
    if(tokens.empty()) return "-Error: Empty command\r\n";

    // std::cout << commandLine << "\n";

    // for(auto &t : tokens) std::cout << t << "\n";
    


    std::string cmd = tokens[0];
    std::transform(cmd.begin() , cmd.end(), cmd.begin() , ::toupper);

    // Connect to Database
    RedisDatabase& db = RedisDatabase::getInstance();
    

    // Check commands
    // Commaon commands
    if(cmd == "PING"){
        return handlePing(tokens);
    }else if(cmd == "DEV"){
        return handleDev();
    }else if(cmd == "ECHO"){
        return handleEcho(tokens);
    }else if(cmd == "FLUSHALL"){
        return handleFlushAll(db);
    }
    // Key-Value Operations
    else if(cmd == "SET"){
        return handleSet(db , tokens);
    } else if(cmd == "GET"){
        return handleGet(db , tokens);
    } else if(cmd == "KEYS"){
        return handleKeys(db);
    } else if(cmd == "TYPE"){
        return handleType(db , tokens);
    } else if(cmd == "DEL" || cmd == "DELETE" || cmd == "UNLINK"){
        return handleDel(db , tokens);
    } else if(cmd == "EXPIRE"){
        return handleExpire(db , tokens);
    } else if(cmd == "RENAME"){
        return handleRename(db , tokens);
    }
    
    // List Operations
    else if(cmd == "LPUSH"){
        return handleLPush(db , tokens);
    } else if(cmd == "RPUSH"){
        return handleRPush(db , tokens);
    } else if(cmd == "LPOP"){
        return handleLPop(db , tokens);
    } else if(cmd == "RPOP"){
        return handleRPop(db , tokens);
    } else if(cmd == "LRANGE"){
        return handleLRange(db , tokens);
    }else if(cmd == "LLEN"){
        return handleLLen(db , tokens);
    }else if(cmd =="LREM"){
        return handleLRem(db , tokens);
    }else if(cmd == "LINDEX"){
        return handleLIndex(db , tokens);
    }else if(cmd == "LSET"){
        return handleLSet(db , tokens);
    }
    // Hash Operations
    
    else {
        return handleUnknownCommand();
    }

}