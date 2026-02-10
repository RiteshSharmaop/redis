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

    RedisServer server(port);
    
    // Background Persistence: dump the database every 300 seconds.  (5*60 save database)
    std::thread persistanceThread([](){
        while(true){
            std::this_thread::sleep_for(std::chrono::seconds(300));
            //  dump the db
            if(!RedisDatabase::getInstance().dump("dump.myrdb")){
                std::cerr << "Error Dumping Database\n";
            }else {
                std::cout << "Database Dumped to dump.my_rdb\n";
            }

        }
    });
    persistanceThread.detach();


    server.run();
    
    return 0;
}