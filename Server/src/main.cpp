#include<iostream>
#include "../include/RedisServer.h"

int main(int argc , char* argv[]) {
    int port = 6379; // default port
    if(argc >= 2){
        port = std::stoi(argv[1]);  //user given port
    }

    RedisServer server(port);
    

    return 0;
}