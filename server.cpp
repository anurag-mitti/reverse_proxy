#include <iostream>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "ThreadPool.hpp"

constexpr int MAX_EVENTS = 64;
constexpr int BUFFER_SIZE = 1024;


void handle_client_read(int epoll_fd, int fd, ThreadPool& pool);

int main() {

    ThreadPool pool(8, std::chrono::milliseconds(5000));

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd == -1) {
        perror("socket");
        return 1;
    }

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    int flags = fcntl(listen_fd, F_GETFL, 0);
    fcntl(listen_fd, F_SETFL, flags | O_NONBLOCK);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(8080);

    if (bind(listen_fd, (sockaddr*)&addr, sizeof(addr)) == -1) {
        perror("bind");
        return 1;
    }

    if (listen(listen_fd, SOMAXCONN) == -1) {
        perror("listen");
        return 1;
    }

    int epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) {
        perror("epoll_create1");
        return 1;
    }

    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = listen_fd;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) == -1) {
        perror("epoll_ctl");
        return 1;
    }

    std::cout << "Server listening on port 8080\n";

    epoll_event events[MAX_EVENTS];

    while (true) {

        int no_fd = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);

        if (no_fd == -1) {
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < no_fd; i++) {

            int fd = events[i].data.fd;

            // accepting new connection -- stays on the main thread,
            // this is cheap and keeps connection bookkeeping simple.
            if (fd == listen_fd) {

                while (true) {

                    sockaddr_in client_addr{};
                    socklen_t client_len = sizeof(client_addr);

                    int client_fd =
                        accept(listen_fd,
                               (sockaddr*)&client_addr,
                               &client_len);

                    if (client_fd == -1) {

                        if (errno == EAGAIN ||
                            errno == EWOULDBLOCK) {
                            break;
                        }

                        perror("accept");
                        break;
                    }

                    int cflags = fcntl(client_fd, F_GETFL, 0);
                    fcntl(client_fd,
                          F_SETFL,
                          cflags | O_NONBLOCK);

                    epoll_event client_ev{};
                    client_ev.events = EPOLLIN;
                    client_ev.data.fd = client_fd;

                    if (epoll_ctl(epoll_fd,
                                  EPOLL_CTL_ADD,
                                  client_fd,
                                  &client_ev) == -1) {

                        perror("epoll_ctl client");
                        close(client_fd);
                        continue;
                    }

                    std::cout
                        << "Client connected fd="
                        << client_fd
                        << std::endl;
                }
            }

            // not a new client -- some fd is ready to read.
            // Hand the actual recv()/processing off to the pool so the
            // main thread never blocks on client work.
            else {

                // Stop watching this fd until the pool finishes reading
                // it. This is what prevents two pool threads from ever
                // calling read() on the same fd at the same time (e.g.
                // if the client sends more data while the first read is
                // still being processed).
                if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr) == -1) {
                    // Fd may have already been removed/closed by another
                    // path; nothing to dispatch in that case.
                    perror("epoll_ctl del (pre-dispatch)");
                    continue;
                }

                pool.enqueue([epoll_fd, fd, &pool]() {
                    handle_client_read(epoll_fd, fd, pool);
                });
            }
        }
    }

    close(listen_fd);
    close(epoll_fd);

    return 0;
}

// Runs on a pool worker thread. Reads whatever is currently available
// on fd, prints it, and either closes the connection or re-registers
// the fd with epoll so the main thread starts watching it again.
void handle_client_read(int epoll_fd, int fd, ThreadPool& /*pool*/) {

    char buffer[BUFFER_SIZE];

    ssize_t bytes = read(fd, buffer, sizeof(buffer) - 1);

    if (bytes <= 0) {

        std::cout << "Client disconnected fd=" << fd << std::endl;
        close(fd); // already removed from epoll before dispatch
        return;
    }

    buffer[bytes] = '\0';

    std::cout << "fd " << fd << " says: " << buffer << std::endl;

    // write(fd, buffer, bytes); // echo back if you want

    // Done with this read -- start watching the fd again so the main
    // thread's epoll_wait() picks up the client's next message.
    epoll_event client_ev{};
    client_ev.events = EPOLLIN;
    client_ev.data.fd = fd;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &client_ev) == -1) {
        perror("epoll_ctl add (post-dispatch)");
        close(fd);
    }
}