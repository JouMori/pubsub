#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <pthread.h>
#include <csse2310a3.h>
#include <csse2310a4.h>
#include <stringmap.h>
#include <semaphore.h>
#include <signal.h>

#define MIN_ARGS 2
#define MAX_ARGS 3
#define BASE_10 10
#define CONNECTIONS_ARG 1
#define PORT_ARG 2
#define MIN_PORT_NUM 1024
#define MAX_PORT_NUM 65535
#define INITIAL_BUFFER_SIZE 3
#define COUNT 1
#define DONT_COUNT 0
#define TWO_TOKENS 2

/* Struct containing the characteristics of a client */
typedef struct Client {
    char* name;
    FILE* toClient;
    FILE* fromClient;
    char** subbedTopics;
    int subCount;
} Client;

/* Struct representing a node in a singly linked list of clients */
typedef struct ClientNode {
    Client client;
    struct ClientNode* next;
} ClientNode;

/* Struct containing data that is shared between each thread */
typedef struct SharedClientInfo {
    StringMap* sm;
    int fd;
    sem_t* mutexLock;
    sem_t* threadLock;
    sigset_t* set;
    int currentConnections;
    int totalConnections;
    int totalPub;
    int totalSub;
    int totalUnsub;
} SharedClientInfo;

/* init_mutex_lock()
 * ----------------
 * Initialises the semaphore responsible for mutual exclusion. 
 *
 * l: the semaphore to be initialised
 *
 * Reference: this code is taken from the Week 8 "race3.c" lecture example
 */
void init_mutex_lock(sem_t* l) {
    sem_init(l, 0, 1);
}

/* init_threadLock()
 * -----------------
 * Initialises the semaphore responsible for connection limiting.
 *
 * l: the semaphore to be initialised
 * connections: the maximum number of connections to be allowed
 *
 * Errors: will fail if connections argument exceeds SEM_VALUE_MAX
 * Reference: this code is adapted from the Week 8 "race3.c" lecture 
 * example
 */
void init_thread_lock(sem_t* l, long connections) {
    sem_init(l, 0, connections);
}

/* take_lock()
 * -----------
 * Decrements the given semaphore.
 *
 * l: the semaphore to decrement
 *
 * Reference: this code is taken from the Week 8 "race3.c" lecture example
 */
void take_lock(sem_t* l) {
    sem_wait(l);
}

/* release_lock()
 * --------------
 * Increments the given semaphore.
 *
 * l: the semaphore to increment
 *
 * Reference: this code is taken from the Week 8 "race3.c" lecture example
 */
void release_lock(sem_t* l) {
    sem_post(l);
}

/* usage_error()
 * -------------
 * Prints the usage error message to standard error, flushes, and exits the 
 * program with status 1.
 */
void usage_error() {
    fprintf(stderr, "Usage: psserver connections [portnum]\n");
    fflush(stderr);
    exit(1);
}

/* socket_error()
 * --------------
 * Prints the socket error message to standard error, flushes, and exits the 
 * program with status 2.
 */
void socket_error() {
    fprintf(stderr, "psserver: unable to open socket for listening\n");
    fflush(stderr);
    exit(2);
}

/* print_invalid()
 * ---------------
 * Prints the invalid message to standard error and flushes.
 */
void print_invalid(Client client) {
    fprintf(client.toClient, ":invalid\n");
    fflush(client.toClient);
}

/* check_spaces_colons_empty()
 * ---------------------------
 * Checks a given string for the presence of spaces and colons, and whether it
 * is empty.
 *
 * str: the string to check
 *
 * Returns: 0 if the string contains spaces or colons or is empty, else 1
 */
int check_spaces_colons_empty(char* str) {
    if (strchr(str, ' ') != NULL ||
	    strchr(str, ':') != NULL ||
	    !strcmp(str, "")) {
	return 0;
    }
    return 1;
}

/* handle_name()
 * -------------
 * Ensures the given name argument is valid and, if the given client does not
 * yet have a name, sets it, otherwise ignores the command.
 *
 * client: the client whose name is to be set
 * name: the name to check and set
 */ 
void handle_name(Client* client, char* name) {
    // Invalid name
    if (!check_spaces_colons_empty(name)) {
	print_invalid(*client);

    // Name does not yet exist - set name
    } else if (client->name == NULL) {
	client->name = name;
    }
}

/* add_subscribed_topic()
 * ----------------------
 * Adds the given topic to the given client's array of subscribed topics. 
 * Dynamically allocates more memory to the array if it is full.
 *
 * client: the client whose array is to be modified
 * bufferSize: the current size of the client's array
 * topic: the topic to be added to the array
 */
void add_subscribed_topic(Client* client, int* bufferSize, char* topic) {
    // Ensure array has sufficient space
    if (client->subCount == *bufferSize) {
	*bufferSize += *bufferSize;
	client->subbedTopics = realloc(client->subbedTopics, 
		sizeof(char*) * *bufferSize);
    }

    // Add to array
    client->subbedTopics[client->subCount] = topic;
    client->subCount++;
}

/* handle_sub()
 * ------------
 * Adds the given client to the given topic's linked list of subscribed 
 * clients (or creates one if none exists). Updates relevant statistics. 
 * Ignores if the client does not have a name or the topic is invalid. 
 *
 * client: the client to be added to the linked list
 * topic: the topic being subscribed to
 * info: struct containing the shared client info (used to access the 
 * StringMap of topics and their subscribed clients, the required semaphore, 
 * and the relevant statistics)
 */
void handle_sub(Client* client, char* topic, SharedClientInfo* info, 
	int* bufferSize) {
    // Invalid topic
    if (!check_spaces_colons_empty(topic)) {
	print_invalid(*client);

    // Name has been set
    } else if (client->name != NULL) {
	ClientNode* item;
	take_lock(info->mutexLock);

	// First client to subscribe to topic - create new linked list
	if (!(item = stringmap_search(info->sm, topic))) {
	    ClientNode* head = malloc(sizeof(struct ClientNode));
	    head->client = *client;
	    head->next = NULL;
	    stringmap_add(info->sm, topic, head);
	    add_subscribed_topic(client, bufferSize, topic);
	    info->totalSub++;

	// Linked list already exists - add to it
	} else {
	    ClientNode* temp = item;

	    // Check if client already subscribed to topic
	    int alreadySubbed = 0;
	    while (temp != NULL) {
		if (temp->client.toClient == client->toClient) {
		    alreadySubbed = 1;
		    break;
		}
		temp = temp->next;
	    }

	    // Add to head of list
	    if (!alreadySubbed) {
		ClientNode* newHead = malloc(sizeof(struct ClientNode));
		newHead->client = *client;
		newHead->next = item;
		stringmap_remove(info->sm, topic);
		stringmap_add(info->sm, topic, newHead);
		add_subscribed_topic(client, bufferSize, topic);
		info->totalSub++;
	    }
	}
	release_lock(info->mutexLock);
    }
}

/* handle_unsub()
 * --------------
 * Removes the given client from the given topic's linked list of subscribed 
 * clients. Updates relevant statistics. Ignores if the client does not have a 
 * name or the topic is invalid or the client is not subscribed to the topic.
 *
 * client: the client to be added to the linked list
 * topic: the topic being subscribed to
 * info: struct containing the shared client info (used to access the 
 * StringMap of topics and their subscribed clients, the required semaphore, 
 * and the relevant statistics)
 * countStat: integer value representing whether or not a successful unsub
 * should be counted in the statistics
 */
void handle_unsub(Client client, char* topic, SharedClientInfo* info, 
	int countStat) {
    // Invalid topic
    if (!check_spaces_colons_empty(topic)) {
	print_invalid(client);

    // Name has been set
    } else if (client.name != NULL) {
	ClientNode* item;
	take_lock(info->mutexLock);

	// List exists
	if ((item = stringmap_search(info->sm, topic)) != NULL) {
	    ClientNode* temp = item;
	    ClientNode* prev;

	    // Client is at head of list
	    if (temp->client.toClient == client.toClient) {
		ClientNode* newHead = temp->next;
		stringmap_remove(info->sm, topic);
		stringmap_add(info->sm, topic, newHead);
		free(temp);
		if (countStat) {
		    info->totalUnsub++;
		}

	    // Iterate through list until client is found
	    } else {
		while (temp != NULL && 
			temp->client.toClient != client.toClient) {
		    prev = temp;
		    temp = temp->next;
		}

		// Client found in list
		if (temp != NULL && countStat) {
		    info->totalUnsub++;
		
	    	    // Remove client from list
	    	    prev->next = temp->next;
	    	    free(temp);
	    	    free(prev);
		}
	    }
	}
	release_lock(info->mutexLock);
    }
}

/* handle_pub()
 * ------------
 * Publishes the given value from the given client to all clients subscribed
 * to the given topic. Updates relevant statistics. Ignores if the topic or
 * value are invalid or the client does not have a name or there are no clients
 * subscribed to the topic.
 *
 * client: the client to publish the message
 * topicAndValue: string containing the topic to publish to and the value to
 * publish
 * info: struct containing the shared client info (used to access the 
 * StringMap of topics and their subscribed clients, the required semaphore, 
 * and the relevant statistics)
 */
void handle_pub(Client client, char* topicAndValue, SharedClientInfo* info) {
    char** pubTokens = split_by_char(topicAndValue, ' ', TWO_TOKENS);
    char* topic = pubTokens[0];
    char* value = pubTokens[1];

    // Invalid topic or publish message
    if (topic == NULL || !check_spaces_colons_empty(topic) || 
	    value == NULL || !strcmp(value, "")) {
	print_invalid(client);

    // Name has been set
    } else if (client.name != NULL) {
	take_lock(info->mutexLock);
	ClientNode* item;

	// At least one client is subscribed
	if ((item = stringmap_search(info->sm, topic))) {
	    ClientNode* temp = malloc(sizeof(struct ClientNode));
	    temp->client = item->client;
	    temp->next = item->next;

	    // Print to each subscribed client
	    while (temp != NULL) {
		fprintf(temp->client.toClient, 
			"%s:%s:%s\n", client.name, topic, value);
		fflush(temp->client.toClient);
		temp = temp->next;
	    }
	    free(temp);
	}
	info->totalPub++;
	release_lock(info->mutexLock);
    }
}

/* clean_up_client()
 * -----------------
 * For the given client, unsubscribes from all subscribed topics, frees 
 * relevant memory, closes relevant file pointers and update relevant 
 * statistics.
 *
 * client: the client to clean up
 * info: struct containing the shared client info (used to access the required 
 * semaphores and the relevant statistics)
 */
void clean_up_client(Client client, SharedClientInfo* info) {
    // Unsub from each subscribed topic
    for (int i = 0; i < client.subCount; i++) {
	handle_unsub(client, client.subbedTopics[i], info, DONT_COUNT);	
    }

    // Free memory
    free(client.subbedTopics);

    // Close file pointers 
    fclose(client.toClient);
    fclose(client.fromClient);   

    // Update statistics
    take_lock(info->mutexLock);
    info->currentConnections--;
    info->totalConnections++;
    release_lock(info->mutexLock);
    release_lock(info->threadLock);
}

/* client_thread()
 * ---------------
 * Thread handling function responsible for handling an individual client.
 * Repeatedly reads single lines from the client and handles each line 
 * accordingly, updating statistics where necessary. Cleans up the client upon
 * disconnection. 
 *
 * arg: the argument passed when creating the thread, in this case the struct
 * containing the shared client info
 *
 * Returns: will always return NULL
 */
void* client_thread(void* arg) {
    SharedClientInfo* info = (SharedClientInfo*) arg;
    int fd2 = dup(info->fd);
    FILE* to = fdopen(info->fd, "w");
    FILE* from = fdopen(fd2, "r");

    int bufferSize = INITIAL_BUFFER_SIZE;
    char** subbedTopics = malloc(sizeof(char*) * bufferSize);

    Client client = {.toClient = to, .fromClient = from, 
	    .subbedTopics = subbedTopics, .subCount = 0};

    take_lock(info->mutexLock);
    info->currentConnections++;
    release_lock(info->mutexLock);

    char* line;
    while ((line = read_line(from)) != NULL) {
	char** tokens = split_by_char(line, ' ', TWO_TOKENS);

	// No second argument received
	if (tokens[1] == NULL) {
    	    print_invalid(client);
	    continue;
	}

	// Handle "name <name>" message
	if (!strcmp(tokens[0], "name")) {
	    handle_name(&client, tokens[1]); 

	// Handle "sub <topic>" message
	} else if (!strcmp(tokens[0], "sub")) {
	    handle_sub(&client, tokens[1], info, &bufferSize);
	
	// Handle "unsub <topic>" message
	} else if (!strcmp(tokens[0], "unsub")) {
	    handle_unsub(client, tokens[1], info, COUNT);
	
	// Handle "pub <topic> <values>" message
	} else if (!strcmp(tokens[0], "pub")) {
	    handle_pub(client, tokens[1], info);
   
	// Message invalid
	} else {
	    print_invalid(client);
	}
    }
    clean_up_client(client, info);
    return NULL;
}

/* sig_thread()
 * ------------
 * Thread handling function responsible for handling the SIGHUP signal.
 * Repeatedly waits until a SIGHUP signal is received and, upon receipt, 
 * prints relevant statistics.
 *
 * arg: the argument passed when creating the thread, in this case the struct
 * containing the shared client info (used to retrieve statistics)
 *
 * Reference: this code was adapted from the pthread_sigmask(3) man page
 */
static void* sig_thread(void* arg) {
    SharedClientInfo* info = (SharedClientInfo*) arg;
    int sig;

    // Repeatedly wait for SIGHUP signal
    while (1) {
	sigwait(info->set, &sig);
	take_lock(info->mutexLock);

	// Print statistics
	fprintf(stderr, "Connected clients:%d\nCompleted clients:%d\n"
		"pub operations:%d\nsub operations:%d\nunsub operations:%d\n", 
		info->currentConnections, 
		info->totalConnections,
		info->totalPub,
		info->totalSub,
		info->totalUnsub);
	fflush(stderr);
	release_lock(info->mutexLock);
    }
    return NULL;
}

/* process_connections()
 * ---------------------
 * Initialises the struct containing the shared client info and creates the 
 * SIGUP signal handling thread. Then repeatedly waits for connections from
 * clients, creating client handling threads as required.
 *
 * fdServer: the listening socket file descriptor
 * connections: the maximum number of connections to be allowed
 *
 * Reference: this code was adapted from the Week 10 "server-multithreaded.c"
 * lecture example and the pthread_sigmask(3) man page
 */
void process_connections(int fdServer, long connections) {
    int fd;
    struct sockaddr_in fromAddr;
    socklen_t fromAddrSize;

    StringMap* sm = stringmap_init();
    sem_t mutexLock; // Lock responsible for ensuring mutual exclusion
    init_mutex_lock(&mutexLock);
    sem_t threadLock; // Lock responsible for connection limiting
    init_thread_lock(&threadLock, connections);

    // Set up SIGHUP signal handling functionality
    pthread_t sigThread; 
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGHUP);
    pthread_sigmask(SIG_BLOCK, &set, NULL);
   
    // Shared data structure between clients
    SharedClientInfo info = {.sm = sm, .mutexLock = &mutexLock, 
	    .threadLock = &threadLock, .set = &set, .currentConnections = 0, 
	    .totalConnections = 0, .totalPub = 0, .totalSub = 0, 
	    .totalUnsub = 0};
    
    // Create dedicated signal handling thread
    pthread_create(&sigThread, NULL, &sig_thread, &info);

    // Repeatedly wait for new client connections
    while (1) {
	fromAddrSize = sizeof(struct sockaddr_in);
	fd = accept(fdServer, (struct sockaddr*) &fromAddr, &fromAddrSize);
	char hostname[NI_MAXHOST];
	getnameinfo((struct sockaddr*) &fromAddr, fromAddrSize,	hostname, 
		NI_MAXHOST, NULL, 0, 0);

	info.fd = fd;
	pthread_t threadID;

	// Connection limit specified
	if (connections > 0) {
	    take_lock(&threadLock);
	}

	// Create client handling thread
	pthread_create(&threadID, NULL, client_thread, &info);
	pthread_detach(threadID);
    }
}

/* open_listen()
 * -------------
 * Opens a given port for listening and prints the port number.
 *
 * port: the port to be opened
 *
 * Returns: the listening socket file descriptor
 * Errors: the program will exit with status 2 if the address could not be 
 * determined, the socket options could not be set, the socket could not be
 * binded, or the socket could not be listened to
 * Reference: this code is adapted from the Week 10 "server-multithreaded.c"
 * and "net4.c" lecture examples
 */
int open_listen(char* port) {
    struct addrinfo* ai = 0;
    struct addrinfo hints;
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    // Could not determine address
    int err;
    if ((err = getaddrinfo(NULL, port, &hints, &ai))) {
	freeaddrinfo(ai);
	socket_error();
    }

    // Create socket and bind it to a port
    int listenFD = socket(AF_INET, SOCK_STREAM, 0);
    int optVal = 1;
    if (setsockopt(listenFD, SOL_SOCKET, SO_REUSEADDR, &optVal, 
	    sizeof(int)) < 0) {
	socket_error(); // Error setting socket option
    }

    if (bind(listenFD, (struct sockaddr*) ai->ai_addr, 
	    sizeof(struct sockaddr)) < 0) {
	socket_error(); // Error binding
    }

    if (listen(listenFD, SOMAXCONN) < 0) {
	socket_error(); // Error listening
    }

    // Obtain port number if ephemeral and print
    struct sockaddr_in ad;
    memset(&ad, 0, sizeof(struct sockaddr_in));
    socklen_t len = sizeof(struct sockaddr_in);
    if (getsockname(listenFD, (struct sockaddr*) &ad, &len)) {
	socket_error();
    }
    fprintf(stderr, "%u\n", ntohs(ad.sin_port));
    fflush(stderr);

    return listenFD;
}

int main(int argc, char* argv[]) {

    // Incorrect number of command line arguments
    if (argc < MIN_ARGS || argc > MAX_ARGS) {
	usage_error();
    }

    // Ensure validity of 'connections' argument
    char* nonNumericConnection;
    long connections = strtol(argv[CONNECTIONS_ARG], &nonNumericConnection, 
	    BASE_10);
    if (strcmp(nonNumericConnection, "") || connections < 0) {
	usage_error();	
    }

    char* port = "0";

    // Port number argument exists
    if (argc == MAX_ARGS) {
	char* nonNumericPort;
	long portNum = strtol(argv[PORT_ARG], &nonNumericPort, BASE_10);
	// Invalid port number
	if (strcmp(nonNumericPort, "") || 
		(portNum && 
		(portNum < MIN_PORT_NUM || portNum > MAX_PORT_NUM))) {
	    usage_error();
	}
	port = argv[PORT_ARG];
    }

    int fdServer = open_listen(port);
    process_connections(fdServer, connections);
    return 0;
}
