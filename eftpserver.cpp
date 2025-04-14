// server code

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>
#include <string>
#include <iostream>
#include <vector>
#include <poll.h>
#include <fstream>
#include <filesystem>
#include <algorithm> 
using namespace std;
namespace fs = filesystem;

#define USERNAME_MAX_LENGTH 32
#define PASSWORD_MAX_LENGTH 32
#define FILENAME_MAX_LENGTH 255
#define SEGMENT_LENGTH 1024
#define ERROR_LENGTH 512
#define AUTH 01
#define RRQ 02
#define WRQ 03
#define DATA 04
#define ACK 05
#define ERROR 06

int n;
int c;
uint16_t id;
/* eftp message structures */
typedef union{
    uint16_t opcode;

    struct{
        uint16_t opcode;
        uint8_t useAndPass[USERNAME_MAX_LENGTH + PASSWORD_MAX_LENGTH + 2];
    } auth;

    struct{
        uint16_t opcode;
        uint16_t session_id;
        uint8_t filename[FILENAME_MAX_LENGTH+1];
    } request;

    struct{
        uint16_t opcode;
        uint16_t session_id;
        uint16_t block_number;
        uint16_t segment_id;
        uint8_t segment_data[SEGMENT_LENGTH];
    } data;

     struct{
        uint16_t opcode;
        uint16_t session_id;
        uint16_t block_number;
        uint16_t segment_id;
    } ack;

     struct{
        uint16_t opcode;
        uint8_t error_string[ERROR_LENGTH];
    } error;

} eftp_message;

ssize_t eftp_recv_message(int sock, eftp_message *m, struct sockaddr_in src, socklen_t *slen){
    ssize_t c;

    if ((c = recvfrom(sock, m, sizeof(*m), 0, (struct sockaddr *) &src, slen))<0)
        perror("server: recvfrom()");
    
    return c;
}

ssize_t eftp_send_data(int sock, uint16_t block_number, uint16_t segment_number, uint8_t data[], int dlen, struct sockaddr_in dest, socklen_t slen){
    eftp_message m;
    m.data.opcode = DATA;
    m.data.block_number = block_number;
    m.data.segment_id = segment_number;
    m.data.session_id = 0;
    memcpy(begin(m.data.segment_data), data, dlen);
    ssize_t c;
    if ((c = sendto(sock, &m, sizeof(m)+(dlen-SEGMENT_LENGTH), 0, (struct sockaddr *) &dest, slen))<0)
        perror("server: sendto()");
    
    return c;
}

int block(int sock, uint16_t block_number, struct sockaddr_in dest, socklen_t slen, FILE *f){
    //vector tracks which segments are acknowledged, array stores everything sent in case it has to be resent
    vector<bool> acks;
    uint8_t datagrams[SEGMENT_LENGTH*8];
    for(int i = 1; i <=8; i++){ 
        uint8_t data[1024] = {0};
        n = fread(data, 1, sizeof(data), f);
    //For testing retransmission
        //int sus = rand() % 2;
        //if(sus == 1)
        eftp_send_data(sock, block_number, i, data, n, dest, slen);
        acks.push_back(false);
        memcpy(&datagrams[((i-1)*1024)], data, n);
        if(n<SEGMENT_LENGTH)
            break;
    }
    int ret = 0;
    int acked = 0;
    while(ret<3 && acked < acks.size()){
        eftp_message r;
        struct pollfd pfd = {.fd = sock, .events = POLLIN};
        //if no acks received in 5000 ms resend all missing acks
        if(poll(&pfd, 1, 5000) == 0){
            ret++;
            for(int i = 0; i<acks.size(); i++){
                if(acks[i] == false){
                    uint8_t data[1024] = {0};
                    memcpy(data, &datagrams[i*1024], SEGMENT_LENGTH);
                    if(i == acks.size()-1){
                        eftp_send_data(sock, block_number, i+1, data, n, dest, slen);
                    }else{
                        eftp_send_data(sock, block_number, i+1, data, SEGMENT_LENGTH, dest, slen);
                    }
                    //cout << "Retransmitting segment " << (i+1) << '\n';
                }
            }
            continue;
        }
        //if this point reached then check ack
        eftp_recv_message(sock, &r, dest, &slen);
        //cout << "Received ACK with ID: " << r.ack.session_id << "Block: " << r.ack.block_number << "Segment ID: " << r.ack.segment_id << '\n';
        if(r.ack.session_id != id || r.ack.block_number != block_number)
            return 1;         
        if(acks[r.ack.segment_id-1])
            return 2;
        if(ret>3){
            return 3;
        }
            
        acked++;
        acks[r.ack.segment_id-1] = true;

    }
    return 0;
}



int main(int argc, char *argv[]){
    //ALL CODE BELOW PARSES COMMAND LINE
    if(argc != 4){
        cout << "Example usage: ./eftpserver [username:password] [listen port] [working directory]\n";
        exit(-1);
    }
    
    string credentials = argv[1];
    string ps = argv[2];
    int port = atoi(ps.c_str());

    string dir = argv[3]; 
    auto path = fs::current_path();
    path += dir;
    try {
        filesystem::current_path(path);
        cout << "Changed directory to " << path << '\n';
    } catch (const exception& e) {
        cout << "Failed to change directory: " << e.what() << '\n';
        return 1;
    }

    if(credentials.find(':') == string::npos){
        cout << "Example usage: ./eftpclient [username:password@ip:port] [upload/download] [filename]\n";
        exit(-1);
    }

    if(port<1 || port>65535){
        cout << "Invalid port number, please provide an integer from 1 to 65535 \n";
        exit(-1);
    }

    int split = credentials.find(':');
    string use = credentials.substr(0, split);
    string pass = credentials.substr(split + 1);
    if(use.length()>32 || pass.length()>32){
        cout << "Maximum username and password lengths are 32 \n";
        exit(-1);
    }

    // END OF COMMAND LINE PARSING
    /////////////////////////////

    while(1){
        int status, sockfd, sess_sockfd, recvlen;
        socklen_t addrlen;

        struct sockaddr_in server_addr, session_addr, client_addr;
        struct sockaddr *server, *session, *client;
        sockfd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        if(sockfd == -1){
            cout << "socket() call failed\n";
            exit(-1);
        }

        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(port);
        server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
        server = (struct sockaddr *) &server_addr;
        client = (struct sockaddr *) &client_addr;
        addrlen = sizeof(server_addr);

        status = bind(sockfd, server, addrlen);
        if(status == -1){
            cout << "bind() call failed\n";
            exit(-1);
        }
        
        eftp_message a;
        //receive auth eftp
        recvlen = recvfrom(sockfd, &a, sizeof(a), 0, client, &addrlen);
        if(a.opcode == AUTH){
            uint8_t  uSent [33] = {0};
            uint8_t  pSent [33] = {0};
            uSent[33] = '\0';
            pSent[33] = '\0';

            memcpy(&uSent, a.auth.useAndPass,sizeof(uSent));
            memcpy(&pSent, &a.auth.useAndPass[33], sizeof(pSent));

            string username = ((char *)uSent);
            string password = ((char *)pSent);
            //check authentication is correct
            if(!(username == use && password == pass)){
                eftp_message err = {0};
                err.error.opcode = ERROR;
                string autherr = "Authentication failed \n";
                cout << "Error: " << autherr;
                copy(autherr.begin(), autherr.end(), begin(err.error.error_string));
                sendto(sockfd, &err, sizeof(err), 0, client, addrlen);
                close(sockfd);
                continue;
            }
        }
        //switch to new port from this point onwards
        sess_sockfd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        if(sess_sockfd == -1){
            cout << "socket() call failed\n";
            exit(-1);
        }
        session_addr.sin_family = AF_INET;
        session_addr.sin_port = htons(0);
        session_addr.sin_addr.s_addr = htonl(INADDR_ANY);
        
        eftp_message ackno = {0};
        ackno.ack.opcode = ACK;
        ackno.ack.block_number = 0;
        ackno.ack.segment_id = 0;
        //get a new session id
        srand(time(NULL));
        id = rand() % 65535 + 1;
        ackno.ack.session_id = id;
        cout << id << '\n';
        sendto(sess_sockfd, &ackno, sizeof(ackno), 0, client, addrlen);
        eftp_message request;
        recvlen = recvfrom(sess_sockfd, &request, sizeof(request), 0, client, &addrlen);
        //get a request eftp and respond accordingly
        if(request.opcode == RRQ){
            //open requested file
            FILE *fp = fopen((const char *)request.request.filename, "rb");
            if (fp == NULL) {
                cout<<"Error opening file\n";
                eftp_message err;
                err.error.opcode = ERROR;
                string filerr = "Could not open requested file\n";
                copy(filerr.begin(), filerr.end(), begin(err.error.error_string));
                sendto(sess_sockfd, &err, sizeof(err), 0, client, addrlen);
                close(sockfd);
                close(sess_sockfd);
                continue;
            }
            //send in blocks
            int count = 1;
            while(!feof(fp)){
                status = block(sess_sockfd, count, client_addr, addrlen, fp);
                count++;
                if(status>0){
                    string serr;
                    if(status == 1){
                        serr = "Invalid ACK received\n";
                    }
                    if(status == 2){
                        serr = "Duplicate ACK receieved \n";
                    }
                    if(status == 3){
                        serr = "Retransmission failed \n";
                    }
                    eftp_message err;
                    err.error.opcode = ERROR;
                    copy(serr.begin(), serr.end(), begin(err.error.error_string));
                    sendto(sess_sockfd, &err, sizeof(err), 0, client, addrlen);
                    close(sockfd);
                    close(sess_sockfd);
                    continue;
                }
            }

        }
        
        if(request.opcode == WRQ){
            ackno = {0};
            ackno.ack.opcode = ACK;

            ackno.ack.block_number = 1;
            ackno.ack.segment_id = 0;
            ackno.ack.session_id = 0;
            sendto(sess_sockfd, &ackno, sizeof(ackno), 0, client, addrlen);
            FILE *fp = fopen((const char *)request.request.filename,"wb");
            eftp_message data = {0};
            bool done = false;
            while(!done){
                //store segments in a larger array (the block)
                uint8_t datagrams[SEGMENT_LENGTH*8] = {0};
                int got = 0;
                //get 8 segments (block)
                for(int i = 1; i <=8; i++){
                        got++;
                        eftp_message m;
                        eftp_message r;
                        c = eftp_recv_message(sess_sockfd, &m, client_addr, &addrlen);

                        if(m.opcode==ERROR){
                            cout << "Client Error: " << m.error.error_string;
                            close(sockfd);
                            close(sess_sockfd);
                            continue;
                        }

                        if(m.opcode==DATA){
                            //put segment into correct part of block
                            memcpy(&datagrams[((m.data.segment_id-1)*1024)], m.data.segment_data, c-8);
                            //send ack 
                            r.ack.opcode = ACK;
                            r.ack.segment_id = m.data.segment_id;
                            r.ack.session_id = 0;
                            r.ack.block_number = m.data.block_number;                
                            sendto(sess_sockfd, &r, sizeof(r), 0, client, addrlen);
                            //quit if a segment is under 1024
                            if(c-8<1024){
                                done = true;
                                break;
                            }     
                        }
                }
                //write to file without exceeding sent amount if last segment is under 1024
                fwrite(datagrams, 1, ((got-1)*SEGMENT_LENGTH)+c-8, fp);
            }
            fclose(fp);

        }

        if(request.opcode == ERROR){
            cout << "Client Error: "<< request.error.error_string << '\n';
        }
        
        close(sockfd);
        close(sess_sockfd);
    }
    
    return 0;
}