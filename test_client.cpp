

#include <iostream>
#include <cstring>
#include <thread>
#include <vector>
#include <chrono>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

void run_client(int id, const std::string& message) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        perror("socket");
        return;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(8080);
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) == -1) {
        perror("connect");
        close(sock);
        return;
    }

    std::string msg = "[client " + std::to_string(id) + "] " + message;

    if (send(sock, msg.c_str(), msg.size(), 0) == -1) {
        perror("send");
        close(sock);
        return;
    }

    std::cout << "client " << id << " sent: " << msg << std::endl;

    close(sock);
}

int main(int argc, char* argv[]) {
    int num_clients = 1;
    std::string message = "hello server";

    if (argc >= 2) num_clients = std::atoi(argv[1]);
    if (argc >= 3) message = argv[2];

    std::vector<std::thread> threads;
    for (int i = 0; i < num_clients; ++i) {
        threads.emplace_back(run_client, i, message);
    }

    for (auto& t : threads) {
        t.join();
    }

    std::cout << "Done. " << num_clients << " connection(s) sent.\n";
    return 0;
}