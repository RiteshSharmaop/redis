# Header File
<netdb.h>	DNS resolution (getaddrinfo, hostent)
<sys/socket.h>	Socket APIs (socket, connect, send)


# redisClient.cpp struct 
struct addrinfo {
    int ai_family;       // IPv4 / IPv6
    int ai_socktype;     // TCP / UDP
    int ai_protocol;     // Protocol
    size_t ai_addrlen;   // Address length
    struct sockaddr *ai_addr; // IP + port
    struct addrinfo *ai_next; // Next result
};