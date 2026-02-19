#include "../include/RedisServer.h"
#include "../include/RedisDatabase.h"
#include<iostream>
#include<thread>
#include<chrono>
int main(int argc , char* argv[]) {
    int port = 6379; // default port
    if(argc >= 2){
        port = std::stoi(argv[1]);  //user given port
    }

    if(RedisDatabase::getInstance().load("dump.myrdb")){
        std::cout << "Database Loaded from dump.myrdb\n";   
    }else {
        std::cout << "No existing database found. Starting with an empty database.\n";  
    }

    RedisServer server(port);
    
    // Background Persistence: dump the database every 100 seconds.
    // todo: try wot 10 second
    std::thread persistanceThread([](){
        while(true){
            std::this_thread::sleep_for(std::chrono::seconds(5));
            //  dump the db
            if(!RedisDatabase::getInstance().dump("dump.myrdb")){
                std::cerr << "Error Dumping Database\n";
            }else {
                std::cout << "Database Dumped to dump.myrdb\n";
            }

        }
    });
    persistanceThread.detach();


    server.run();
    
    return 0;
}