//client code

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

//method to receive packets containing eftp message 
ssize_t eftp_recv_message(int sock, eftp_message *m, struct sockaddr_in src, socklen_t *slen){
    ssize_t c;

    if ((c = recvfrom(sock, m, sizeof(*m), 0, (struct sockaddr *) &src, slen))<0)
        perror("server: recvfrom()");
    
    return c;
}

//method to send a data eftp
ssize_t eftp_send_data(int sock, uint16_t session_id, uint16_t block_number, uint16_t segment_number, uint8_t data[], int dlen, struct sockaddr_in dest, socklen_t slen){
    eftp_message m;
    m.data.opcode = DATA;
    m.data.block_number = block_number;
    m.data.segment_id = segment_number;
    m.data.session_id = session_id;
    memcpy(begin(m.data.segment_data), data, dlen);
    ssize_t c;
    if ((c = sendto(sock, &m, sizeof(m)+(dlen-SEGMENT_LENGTH), 0, (struct sockaddr *) &dest, slen))<0)
        perror("server: sendto()");
    
    return c;
}

//method to send a block
int block(int sock, uint16_t session_id, uint16_t block_number, struct sockaddr_in dest, socklen_t slen, FILE *f){
    //vector tracks which segments are acknowledged, array stores everything sent in case it has to be resent
    vector<bool> acks;
    uint8_t datagrams[SEGMENT_LENGTH*8];
    for(int i = 1; i <=8; i++){ 
        uint8_t data[1024] = {0};
        n = fread(data, 1, sizeof(data), f);
    //For testing retransmission
        //int sus = rand() % 2;
        //if(sus == 1)
        eftp_send_data(sock, session_id, block_number, i, data, n, dest, slen);
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
                        eftp_send_data(sock, session_id, block_number, i+1, data, n, dest, slen);
                    }else{
                        eftp_send_data(sock, session_id, block_number, i+1, data, SEGMENT_LENGTH, dest, slen);
                    }
                    //cout << "Retransmitting segment " << (i+1) << '\n';
                }
            }
            continue;
        }
        //if this point reached then check ack 
        eftp_recv_message(sock, &r, dest, &slen);
        //cout << "Received ACK with ID: " << r.ack.session_id << "Block: " << r.ack.block_number << "Segment ID: " << r.ack.segment_id << '\n';
        if(r.ack.block_number != block_number)
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
        cout << "Example usage: ./eftpclient [username:password@ip:port] [upload/download] [filename]\n";
        exit(-1);
    }

    string credentials = argv[1];
    string updown = argv[2];
    string file = argv[3]; 

    if(credentials.find(':') == string::npos){
        cout << "Example usage: ./eftpclient [username:password@ip:port] [upload/download] [filename]\n";
        exit(-1);
    }

    int split = credentials.find(':');
    string use = credentials.substr(0, split);
    string rest = credentials.substr(split + 1);

    if(rest.find('@') == string::npos){
        cout << "Example usage: ./eftpclient [username:password@ip:port] [upload/download] [filename]\n";
        exit(-1);
    }

    split = rest.find('@');
    string pass = rest.substr(0, split);
    string portarg = rest.substr(split + 1);

    if(use.length()>32 || pass.length()>32){
        cout << "Maximum username and password lengths are 32 \n";
        exit(-1);
    }

    if(portarg.find(':') == string::npos){
        cout << "Example usage: ./eftpclient [username:password@ip:port] [upload/download] [filename]\n";
        exit(-1);
    }

    split = portarg.find(':');
    string ip = portarg.substr(0, split);
    int port = atoi(portarg.substr(split + 1).c_str());
    if(port<1 || port>65535){
        cout << "Invalid port number, please provide an integer from 1 to 65535 \n";
        exit(-1);
    }

    bool protocol;
    transform(updown.begin(), updown.end(), updown.begin(), ::tolower);
    if(updown == "upload" || updown == "u" || updown == "up"){
        protocol = true;
    }else if(updown == "download" || updown == "d" || updown == "down"){
        protocol = false;
    }else{
        cout << "Example usage: ./eftpclient [username:password@ip:port] [upload/download] [filename]\n";
        exit(-1);
    }

    // END OF COMMAND LINE PARSING
    ////////////////////////

    int status, sockfd, recvlen;
    socklen_t addrlen;

    struct sockaddr_in my_addr, server_addr, server_new_addr;
    struct sockaddr *me, *server, *server_new;

    sockfd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if(sockfd == -1){
        cout << "socket() call failed\n";
        exit(-1);
    }
    my_addr.sin_family = AF_INET;
    my_addr.sin_port = htons(0);
    my_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    me = (struct sockaddr *) &my_addr;
    addrlen = sizeof(my_addr);

    status = bind(sockfd, me, addrlen);
    if(status == -1){
        cout << "bind() call failed\n";
        exit(-1);
    }

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, ip.c_str(), &server_addr.sin_addr);
    server = (struct sockaddr *) &server_addr;
    addrlen = sizeof(server_addr);

    //send auth eftp
    eftp_message authPack = {0};
    authPack.auth.opcode = AUTH;
    
    copy(use.begin(), use.end(), begin(authPack.auth.useAndPass));
    copy(pass.begin(), pass.end(), begin(authPack.auth.useAndPass)+USERNAME_MAX_LENGTH+1);
    sendto(sockfd, &authPack, sizeof(authPack), 0, (struct sockaddr *) &server_addr, addrlen);

    //using new port from this point on
    server_new = (struct sockaddr *) &server_new_addr;
    eftp_message reply;
    recvlen = recvfrom(sockfd, &reply, sizeof(reply), 0, server_new, &addrlen);

    //print error from server and exit
    if(reply.opcode == ERROR){
        cout << "Server Error: " << reply.error.error_string;
        exit(-1);
    }
    //get the session id from the first ack
    if(reply.opcode == ACK){
        id = reply.ack.session_id;
        //cout << id << '\n';
    }

    //send a wrq or rrq request
    eftp_message request = {0};
    if(protocol){
        request.request.opcode = WRQ;
        copy(file.begin(), file.end(), begin(request.request.filename));
        sendto(sockfd, &request, sizeof(request), 0, server_new, addrlen);
        eftp_message reply;
        recvlen = recvfrom(sockfd, &reply, sizeof(reply), 0, server_new, &addrlen);

        if(reply.opcode == ERROR){
            cout << "Server Error: " << reply.error.error_string;
            exit(-1);
        }

        if(reply.opcode == ACK){
            if(reply.ack.segment_id!=0 || reply.ack.session_id!=0 || reply.ack.block_number!=1){
                eftp_message err;
                err.error.opcode = ERROR;
                string aerr = "Invalid ACK received1\n";
                cout << "Error: " << aerr;
                copy(aerr.begin(), aerr.end(), begin(err.error.error_string));
                sendto(sockfd, &err, sizeof(err), 0, server_new, addrlen);
                close(sockfd);
                exit(1);
            }
        }
        FILE *fp = fopen(file.c_str(), "rb");
        if (fp == NULL) {
            cout<<"Error opening file\n";
            eftp_message err;
            err.error.opcode = ERROR;
            string filerr = "Could not open requested file\n";
            copy(filerr.begin(), filerr.end(), begin(err.error.error_string));
            sendto(sockfd, &err, sizeof(err), 0, server_new, addrlen);
            close(sockfd);
            exit(1);
        }
        //keep sending blocks until file end
        int count = 1;
        while(!feof(fp)){
            status = block(sockfd, id, count, server_new_addr, addrlen, fp);
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
                cout << "Error: " << serr;
                copy(serr.begin(), serr.end(), begin(err.error.error_string));
                sendto(sockfd, &err, sizeof(err), 0, server_new, addrlen);
                close(sockfd);
                exit(-1);
            }
        }
    }else{
        request.request.opcode = RRQ;
        copy(file.begin(), file.end(), begin(request.request.filename));
        sendto(sockfd, &request, sizeof(request), 0, server_new, addrlen);
        FILE *fp = fopen(file.c_str(),"wb");
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
                c = eftp_recv_message(sockfd, &m, server_new_addr, &addrlen);

                if(m.opcode==ERROR){
                    cout << "Server Error: " << m.error.error_string;
                    exit(1);
                }

                if(m.opcode==DATA){
                    //put segment into correct part of block
                    memcpy(&datagrams[((m.data.segment_id-1)*1024)], m.data.segment_data, c-8);
                    //send ack 
                    r.ack.opcode = ACK;
                    r.ack.segment_id = m.data.segment_id;
                    r.ack.session_id = id;
                    r.ack.block_number = m.data.block_number;                
                    sendto(sockfd, &r, sizeof(r), 0, server_new, addrlen);
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

    close(sockfd);
    return 0;
}