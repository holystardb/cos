#define WIN32_LEAN_AND_MEAN
#define _CRT_SECURE_NO_WARNINGS
#define _WINSOCK_DEPRECATED_NO_WARNINGS

#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include <malloc.h>

#include <Windows.h>
#include <WinSock2.h>

#pragma comment(lib, "ws2_32.lib")

char g_server_host[256];
int  g_server_port = 8500;

static int get_options(int argc, char **argv)
{
    char *pos, *progname;

    progname = argv[0];

    while (--argc >0 && *(pos = *(++argv)) == '-')
    {
        switch (*++pos)
        {
        case 'h':
            strcpy(g_server_host, ++pos);
            break;
        case 'p':
            g_server_port = atoi(++pos);
            break;
        default:
            printf("Usage: client -h 127.0.0.1 -p 8500\n");
            exit(0);
        }
    }

    return 0;
} /* get_options */

int main(int argc, char *argv[]) 
{
    int     cnt;
    WSADATA wd;
    WSAStartup(MAKEWORD(2, 2), &wd);

    strcpy(g_server_host, "127.0.0.1");
    get_options(argc, argv);

    //�����ͻ����׽���
    SOCKET sktCli = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

    struct sockaddr_in addrSer = {0};
    addrSer.sin_family = AF_INET;//Ҫ�ӵķ�������IPV4Э��
    addrSer.sin_port = htons(g_server_port);//Ҫ���ӵķ������Ķ˿�
    addrSer.sin_addr.s_addr = inet_addr(g_server_host);//Ҫ���ӵķ�������IP
    //���ӷ�����
    if (0 != connect(sktCli, (struct sockaddr *)&addrSer, sizeof(addrSer)))
    {
        fprintf(stderr, "[client] cannot connect to server(%s)\n", g_server_host);
        fflush(stderr);
        Sleep(3);
        return 0;
    }

    char buf[1024];
    while (1) {
        printf(">>");
        scanf("%s", buf);
        if (0 == strcmp(buf, "exit"))
        {
            printf("[client] byte");
            fflush(stderr);
            Sleep(3);
            return 0;
        }

        cnt = send(sktCli, buf, strlen(buf) + 1, 0);
        if (-1 == cnt) //���������������
        {
            printf("[client] Error for send data to server");
            fflush(stderr);
            Sleep(3);
            return 0;
        }

        buf[0] = '\0';
        cnt = recv(sktCli, buf, sizeof(buf), 0);
        if (-1 == cnt) //���ܷ��������͵�����
        {
            printf("[client] Error for receive data from server");
            fflush(stderr);
            Sleep(3);
            return 0;
        }
        buf[cnt] = '\0';
        printf("[server] %s\n", buf);
    }

    WSACleanup();
    return 0;
}