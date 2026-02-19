#include "../include/RedisDatabase.h"

RedisDatabase& RedisDatabase::getInstance(){
    static RedisDatabase instance;
    return instance;
}


// Comman Commands
bool RedisDatabase::flushAll(){
    // Lock the database while flushing to ensure thread safety
    std::lock_guard<std::mutex> lock(db_mutex);
    // Clear all data from the database
    kv_store.clear();
    list_store.clear();
    hash_store .clear();
    return true;
}

// Key/Value Operations
void RedisDatabase::set(const std::string &key , const std::string &value){
    std::lock_guard<std::mutex> lock(db_mutex);
    // Set the value for the key in kv_store
    kv_store[key] = value;
}
bool RedisDatabase::get(const std::string &key , std::string &value){
    std::lock_guard<std::mutex> lock(db_mutex);
    // Check if key exists in kv_store
    auto it = kv_store.find(key);
    if(it != kv_store.end()){
        // Key exists, return value
        value = it->second;
        return true;
    }
    return false;
}
std::vector<std::string> RedisDatabase::keys(){
    std::lock_guard<std::mutex> lock(db_mutex);
    std::vector<std::string> keys;
    // Collect keys from all stores
    for(const auto &kv : kv_store){
        keys.push_back(kv.first);
    }
    for(const auto &kv : list_store){
        keys.push_back(kv.first);
    }
    for(const auto &kv : hash_store){
        keys.push_back(kv.first);
    }
    return keys;
}
std::string RedisDatabase::type(const std::string &key){
    std::lock_guard<std::mutex> lock(db_mutex);
    // Check in order: string, list, hash
    if(kv_store.find(key) != kv_store.end()){
        return "string";
    }else if(list_store.find(key) != list_store.end()){
        return "list";
    }else if(hash_store.find(key) != hash_store.end()){
        return "hash";
    }
    return "none";
}
// expire 
bool RedisDatabase::del(const std::string &key){
    std::lock_guard<std::mutex> lock(db_mutex);
    // Remove from all stores and expiry map
    bool removed = false;
    if(kv_store.erase(key) > 0) removed = true;
    if(list_store.erase(key) > 0) removed = true;
    if(hash_store.erase(key) > 0) removed = true;
    expiry_map.erase(key);
    return removed;
}
bool RedisDatabase::expire(const std::string &key , std::string seconds){
    std::lock_guard<std::mutex> lock(db_mutex);
    // Checking key exists or not
    bool exists = kv_store.find(key) != kv_store.end() || list_store.find(key) != list_store.end() || hash_store.find(key) != hash_store.end();
    if(!exists) return false;

    try {   
        int sec = std::stoi(seconds);
        // Set the expiry time for the key
        expiry_map[key] = std::chrono::steady_clock::now() + std::chrono::seconds(sec);
        return true;
    }catch(const std::exception &e){
        return false; // invalid seconds
    }
}
// rename 
bool RedisDatabase::rename(const std::string &oldKey , const std::string newKey){
    std::lock_guard<std::mutex> lock(db_mutex);
    if(oldKey == newKey) return false; // No operation needed

    // Check if oldKey exists
    bool exists = kv_store.find(oldKey) != kv_store.end() || list_store.find(oldKey) != list_store.end() || hash_store.find(oldKey) != hash_store.end();
    if(!exists) return false;

    // Check if newKey already exists
    if(kv_store.find(newKey) != kv_store.end() || list_store.find(newKey) != list_store.end() || hash_store.find(newKey) != hash_store.end()){
        return false; // newKey already exists
    }

    // Rename in kv_store
    auto kv_it = kv_store.find(oldKey);
    if(kv_it != kv_store.end()){
        kv_store[newKey] = kv_it->second;
        kv_store.erase(kv_it);
        return true;
    }

    // Rename in list_store
    auto list_it = list_store.find(oldKey);
    if(list_it != list_store.end()){
        list_store[newKey] = list_it->second;
        list_store.erase(list_it);
        return true;
    }

    // Rename in hash_store
    auto hash_it = hash_store.find(oldKey);
    if(hash_it != hash_store.end()){
        hash_store[newKey] = hash_it->second;
        hash_store.erase(hash_it);
        return true;
    }

    auto expiry_it = expiry_map.find(oldKey);
    if(expiry_it != expiry_map.end()){
        expiry_map[newKey] = expiry_it->second;
        expiry_map.erase(expiry_it);
    }

    return false; // Should not reach here
}




// Key-Value Operations
// List Operations
// Hash Operations
    

/*
Very simple text based persistence: each line encodes a record

Memory -> File - dump()
File -> Memory - load()

To Handle Server  Data
K = Key Value
L = List
H = Hash

*/

bool RedisDatabase::dump(const std::string &filename){
    std::lock_guard<std::mutex> lock(db_mutex);
    std::ofstream ofs(filename , std::ios::binary);
    if(!ofs) return false;


    // key value
    for(const auto &kv : kv_store){
        ofs << "K " << kv.first << " " << kv.second << "\n"; 
    }
    
    // list
    for(const auto &kv : list_store){
        ofs << "L " << kv.first;
        for(const auto &item : kv.second){
            ofs << " " << item;
        }
        ofs << "\n";
    }
    
    // hash
    for(const auto &kv : hash_store){
        ofs << "H " << kv.first;
        for(const auto &field_val : kv.second){
            ofs << " " << field_val.first << ":" << field_val.second;
        }
        ofs << "\n";
        
    }
    
    
    return true;
}
bool RedisDatabase::load(const std::string &filename){
    std::lock_guard<std::mutex> lock(db_mutex);
    std::ifstream ifs(filename , std::ios::binary);
    if(!ifs) return false;

    kv_store.clear();
    list_store.clear();
    hash_store.clear();

    std::string line;
    while(std::getline(ifs , line)) {
        std::istringstream iss(line);
        char type;
        iss >> type;
        if(type == 'K'){
            std::string key , value;
            iss >> key >> value;
            kv_store[key] = value;
        }else if(type == 'L') {
            std::string key;
            iss >> key;
            std::string item;
            std::vector<std::string> list;
            while(iss >> item){
                list.push_back(item); 
            }
            list_store[key] = list;
        }else if(type == 'H'){
            std::string key;
            iss >> key;
            std::unordered_map<std::string , std::string> hash;
            std::string pair;
            while(iss >> pair){
                auto pos = pair.find(':');

                if(pos != std::string::npos){
                    std::string field = pair.substr(0 , pos);
                    std::string value = pair.substr(pos+1);
                    hash[field] = value;
                }
            }
            hash_store[key] = hash;

        }
    }
    return true;
}