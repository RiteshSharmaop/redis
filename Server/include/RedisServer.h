#ifndef REDIA_SERVER_H
#define REDIA_SERVER_H

#include<string>
#include<atomic>
#include<iostream>
#include<sys/socket.h>
#include<unistd.h>
#include<netinet/in.h>
#include<thread>
#include<vector>
#include<cstring>
#include<signal.h>

class RedisServer{
    public:
    RedisServer(int port);
    void run();
    void shutdown();

    private:
        int port;
        int server_socket;
        std::atomic<bool> running;

        // Setup signal handling for gracefull shutdown(ctr + c)
        void setupSignalHandler();
};


#endif //end REDIA_SERVER_H