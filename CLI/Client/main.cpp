#include"CLI.h"
#include<iostream>
#include<string>



int main(int argc , char* argv[]) {
    std::string host = "127.0.0.1";
    int port = 6379;
    int i = 1;
    std::vector<std::string> commandArgs;

    /*
    We are reading command-line input given by the user 
    and converting it into usable C++ variables (host and port) for our program.
    */
    // Parse command-line args for -h and -p
    while(i < argc){
        std::string arg = argv[i];
        
        if(arg == "-h" && i+1 < argc){ // -h 127.0.0.1
            host = argv[++i];
        }else if(arg == "-p" && i+1 < argc) {
            port = std::stoi(argv[++i]);
        }else {
            // remaining args
            while(i < argc){
                commandArgs.push_back(argv[i]);
                i++;
            }
            break;
            // std::cerr << "❌ Invalid argument: " << arg << "\n";
            // std::cerr << "Usage: ./app [-h host] [-p port]\n";
            // return 1;  // exit with error
        }
        
        ++i;
    }
    std::cout << "Connecting to " << host << ":" << port << "\n";

    
    // Handle REPL and one-shot command mode
    CLI cli(host , port);
    // if(!commandArgs.empty()){
    //     cli.executeCommand(commandArgs);  // execute command immediatly
    // }else {
        // }
    cli.run(commandArgs);  // launche REPL mode
    return 0;


}